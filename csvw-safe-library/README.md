# CSVW-SAFE Utility Library

This library provides Python utilities for generating, validating, and testing CSVW-SAFE metadata and associated dummy datasets for differential privacy (DP) development and safe data modeling workflows.

It includes five main scripts:

1. `make_metadata_from_data.py`
2. `make_dummy_from_metadata.py`
3. `validate_metadata.py` 
4. `validate_metadata_shacl.py` (requires `pyshacl`)
5. `assert_same_structure.py`

![Overview](../images/utils_scripts.png)

In addition, two other scripts are available for conversion of csvw-safe metadata to smartnoise sql and opendp libraries:
5. `csvw_to_smartnoise_sql.py` converts the metadata to the format expected in smartnoise-sql
6. `assert_same_structure.py` prepares a context object for opendp with margin and information extracted from csvw-metadata format.

---

## Installation

Install Python 3.11+ and

```bash
pip install csvw-safe
```

or for development: 
```
git clone https://github.com/dscc-admin-ch/csvw-safe-library.git
cd csvw-safe-library
pip install -e .[dev]
```


## Scripts Overview

### 1. **`make_metadata_from_data.py`**
#### Purpose

Automatically generate baseline CSVW-SAFE metadata from an existing dataset.

This script infers:
- Column datatypes
- Nullability and missingness rates
- Numeric bounds (min/max)
- Optional continuous partitions
- Contribution constraints (DP-oriented metadata)
- Optional column dependencies
- Optional column grouping metadata

**Important**: This tool is for automated metadata *drafting only*. All outputs must be manually reviewed before production or publication.

#### CLI Usage Examples

```bash
# Basic usage
python make_metadata_from_data.py data.csv --privacy_unit user_id,
```

It is possible to compute dependencies (bigger, depends on, etc) between columns with
```bash
# Enable dependency detection (default: True)
python make_metadata_from_data.py data.csv \
  --privacy_unit user_id \
  --with_dependencies True
```

It is possible to describe partitions level of continuous data if public bounds are provided
```bash
# Add continuous partitions
python make_metadata_from_data.py data.csv \
  --privacy_unit user_id \
  --continuous_partitions '{"age": [0, 18, 30, 50, 100]}'
```

It is possible to describe group of columns information (like after grouping by a list of columns) to have their metadata
```bash
# Define column groups
python make_metadata_from_data.py data.csv \
  --privacy_unit user_id \
  --column_groups '[["age", "income"], ["city", "country"]]'
```

Some flexibility is given to decide the level of description
Valid default contribution levels are:
- "table": TODO DESCRIBE BEHAVIOUR
- "column": TODO DESCRIBE BEHAVIOUR
- "partition": TODO DESCRIBE BEHAVIOUR

```bash
# Set default contribution level
python make_metadata_from_data.py data.csv \
  --privacy_unit user_id \
  --default_contributions_level table

# Column-specific contribution overrides
python make_metadata_from_data.py data.csv \
  --privacy_unit user_id \
  --fine_contributions_level '{"age": "column", "income": "partition"}'
```

Save output to specific file
```bash
python make_metadata_from_data.py data.csv \
  --privacy_unit user_id \
  --output my_metadata.json
```

Notes
- Datetime columns are automatically inferred using pandas.to_datetime.
- Numeric bounds are computed only for non-string columns.
- Contribution levels control per-privacy-unit contribution constraints.
- Dependency detection may increase runtime on large datasets.
- Output is a JSON-serializable CSVW-SAFE metadata structure.

#### Future plans:
- Allow a DP vs non-DP mode (with/without) DP attributes
- Allow finer contribution level descrition (for now column level is very broad)

### 2. **`make_dummy_from_metadata.py`**

#### Purpose

Generate a synthetic dummy dataset from CSVW-SAFE metadata.

The generator creates structured data that follows the declared metadata constraints, including:
- Column datatypes
- Numeric and categorical partitions
- Optional dependency structure between columns
- Nullable proportions
- Column-group constraints (when provided)

> ⚠️ **Important**: This tool produces *synthetic structural data only*.  
> It does not preserve semantic meaning or real-world correlations beyond what is encoded in metadata.

---

### Output Guarantees

The generated dataset:
- Respects declared column schema (datatypes)
- Respects partition definitions (categorical + continuous)
- Respects numeric bounds when defined
- Applies nullable proportions per column
- Optionally respects column-group partition constraints
- Produces reproducible results via random seed

---

### Typical Use Cases

- Unit testing of CSVW-SAFE and DP pipelines
- Schema validation without real data access
- Debugging metadata-driven transformations
- Synthetic data generation for integration tests

---

### CLI Usage Examples
Basic example with 100 rows:
```bash
# Basic
python make_dummy_from_metadata.py metadata.json --output dummy.csv
```

Set a seed (seed=42) and a number of rows (rows=1000) for a reproducible example:
```bash
python make_dummy_from_metadata.py metadata.json \
  --rows 1000 \
  --seed 42 \
  --output dummy.csv
```

### 3. **`validate_metadata.py`**

#### Purpose

Validate a CSVW-SAFE metadata file against the internal metadata schema.

This tool ensures that a metadata file is structurally correct and conforms to the expected CSVW-SAFE specification as defined by the internal `TableMetadata` model.

It is primarily used as a **preflight validation step** before using metadata for:
- dummy dataset generation
- DP pipeline configuration
- downstream schema-driven processing

---

### Validation Behavior

This validator performs **schema-level validation only**, including:
- Required fields presence
- Type correctness
- Structural consistency of metadata objects
- Compatibility with the `TableMetadata` model

Validation is implemented via a Pydantic model (`TableMetadata.from_dict`).

### Output Behavior

- If metadata is valid → script exits silently (no output)
- If metadata is invalid → raises a validation exception and exits with error

### CLI Usage
```bash
python validate_metadata.py metadata.json
```


### 4. **`validate_metadata_shacl.py`**

#### Purpose

Validate CSVW-SAFE metadata using a **SHACL constraint schema**.

This tool performs structural validation of metadata expressed in **JSON-LD format** against a SHACL shapes graph defined in **Turtle format**.

It is the most strict validation layer in the CSVW-SAFE toolchain, intended to ensure full compliance with RDF-based constraints.

#### Validation Scope

This validator checks:
- RDF structural consistency of metadata (JSON-LD parsing)
- Constraint satisfaction against SHACL shapes
- Class/property-level restrictions defined in the schema
- Cross-field structural rules defined in the SHACL graph

> Unlike `validate_metadata.py`, this tool performs **formal SHACL validation**, not just schema validation.


Python usage
```bash
python validate_metadata_shacl.py metadata.jsonld shapes.ttl
```

Validation output
On success: SHACL validation SUCCESSFUL
On failure: SHACL validation FAILED with a <detailed SHACL report>


Typical Use Cases
- Formal compliance validation of CSVW-SAFE metadata
- CI/CD enforcement of metadata correctness
- Pre-deployment validation in RDF-based pipelines
- Ensuring compatibility with external SHACL-aware systems

Notes
- Metadata must be valid JSON-LD RDF
- SHACL shapes must be valid Turtle RDF
- This is the strictest validation layer
- More expressive than Pydantic-based validation (validate_metadata.py)

### 5. **`assert_same_structure.py`**

### 5. **`validate_dummy_structure.py`**

#### Purpose

Verify that a generated dummy CSV preserves the **structural properties** of an original dataset under the CSVW-SAFE assumptions.

This tool ensures that synthetic data produced by `make_dummy_from_metadata.py` remains **schema-compatible** with the original dataset used to derive metadata.

> ⚠️ This validator checks **structure only**.  
> It does **not** assess statistical similarity or data realism.

---

#### What is validated

The tool checks:

- Column names and ordering
- Inferred CSVW-SAFE datatypes
- Nullability constraints (required vs optional columns)
- Optional categorical domain compatibility (subset check)

#### What is NOT validated

- Statistical similarity between datasets
- Distributional properties
- Correlation structure
- Semantic correctness of values


### Core Validation Logic

#### Column structure

Ensures that both datasets share identical schema:

- Same column names
- Same column ordering


#### Datatype compatibility

Each column is type-checked using CSVW-SAFE inference:

- Datatypes are inferred via `infer_xmlschema_datatype`
- Integer subtype differences are tolerated (e.g., small vs large integer variants)


#### Nullability check

Validates whether required/optional status is preserved:

- A column is considered **required** if it has no missing values
- Both datasets must agree on required vs optional status per column


#### Categorical validation (optional)

If enabled, ensures:

- All values in dummy dataset are a **subset** of original dataset values
- Uses `is_categorical()` to detect categorical columns

### CLI Usage

#### Basic validation
```bash
python assert_same_structure.py original.csv dummy.csv
```

Skip categorical validation
```
python validate_dummy_structure.py original.csv dummy.csv --no-categories
```

Typical Use Cases
- Validate synthetic dataset generation correctness
- Regression testing for metadata-driven pipelines
- Ensuring structural integrity in DP synthetic data workflows
- Debugging mismatches between metadata and generated datasets
Notes
- This tool is intentionally strict on schema alignment but lenient on integer type variations
- Designed to validate synthetic structural fidelity, not realism
- Works best in combination with: make_metadata_from_data.py and make_dummy_from_metadata.py

## Typical Workflow

### Via CLI
1. Generate baseline metadata from the original dataset:
```
python make_metadata_from_data.py data.csv --id user_id --mode fine
```

2. Review manually with a data expert and approve metadata for safety and governance compliance.
Optionnaly after removing private information, run (to validate metadata format)
```
python scripts/validate_metadata_shacl.py metadata.json csvw-safe-constraints.ttl
```
and with shacl constraints:
```
python validate_metadata.py metadata.json --shacl csvw-safe-constraints.ttl
```

3. Generate a dummy dataset from the approved metadata:
```
python make_dummy_from_metadata.py metadata.json --rows 1000 --output dummy.csv
```

4. Verify that the dummy matches the original structure:
```
python assert_same_structure.py data.csv dummy.csv
```


### Python API Workflow
```
import pandas as pd
from csvw_safe.make_metadata_from_data import make_metadata_from_data

df = pd.read_csv("data.csv")

# Generate metadata
metadata = make_metadata_from_data(df, csv_url="data.csv", individual_col="user_id")

```
MANUAL REVIEW OF METADATA. VERIFY ONLY PUBLIC INFORMATION. REMOVE OTHERWISE.

```
from csvw_safe.validate_metadata import validate_metadata
from csvw_safe.validate_metadata_shacl import validate_metadata_shacl
from csvw_safe.make_dummy_from_metadata import make_dummy_from_metadata
from csvw_safe.assert_same_structure import assert_same_structure

# Validate metadata
errors = validate_metadata(metadata)
errors = validate_metadata_shacl(metadata)

# Generate dummy dataset
dummy_df = make_dummy_from_metadata(metadata, nb_rows=500)

# Assert structure
assert_same_structure(df, dummy_df)
```

## Tests
```
cd csvw-safe-library
pip install -e .[dev,dp_lib,rdf]
pytest --cov=csvw_safe --cov-report=term-missing tests/
```

## Notes

These scripts assist safe data modeling workflows; they DO NOT replace governance decisions on what is public information or not.

IMPORTANT: Automatically generated metadata may contain sensitive information — MANUAL REVIEW IS ALWAYS REQUIRED before further steps.

The dummy dataset is intended for development, testing, and pipeline verification, not analysis of real individuals.

# Directory Structure

```
examples/
└─ Notebooks.ipynb                      # Example notebooks demonstrating CSVW-SAFE workflows

src/csvw_safe/
    ├─ __init__.py                          # Package initializer for CSVW-SAFE library

    ├─ make_metadata_from_data.py          # Generate CSVW-SAFE metadata automatically from a dataset
    ├─ make_dummy_from_metadata.py         # Generate synthetic dummy datasets from CSVW-SAFE metadata
    ├─ validate_metadata.py                # Validate metadata using internal schema (TableMetadata model)
    ├─ validate_metadata_shacl.py          # Validate metadata using SHACL constraints via RDF graphs
    ├─ assert_same_structure.py            # Compare original and dummy CSVs for structural consistency

    ├─ csvw_to_opendp_context.py           # Convert CSVW-SAFE metadata into OpenDP analysis context
    ├─ csvw_to_opendp_margins.py           # Translate CSVW-SAFE metadata into OpenDP margin definitions
    ├─ csvw_to_smartnoise_sql.py           # Convert CSVW-SAFE metadata into SmartNoise SQL format

    ├─ generate_series.py                  # Generate synthetic column values based on metadata rules
    ├─ metadata_structure.py               # Core data models defining CSVW-SAFE metadata schema
    ├─ constants.py                        # Shared constants used across metadata pipeline
    ├─ datatypes.py                        # Datatype inference and CSVW-SAFE type utilities
    └─ utils.py                            # General helper utilities for metadata processing
tests/                                  # Unit and integration tests for CSVW-SAFE library

pyproject.toml                         # Project configuration and dependencies
README.md                              # Project overview and documentation entry point
run_linter.sh                          # Script to run linting and style checks
```

