# CSVW-SAFE Python Library

The `csvw-safe` Python package provides utilities for generating, validating, and using CSVW-SAFE metadata for safe data sharing and Differential Privacy (DP) workflows.

The library helps:

- Generate metadata from datasets
- Validate metadata structure and SHACL constraints
- Generate structurally valid dummy datasets
- Convert metadata into DP library formats
- Test structural consistency between datasets

The library is designed for:

- Differential Privacy workflows
- Safe metadata publication
- Synthetic data generation
- DP query calibration
- Testing and prototyping

---

## Main Features

The library includes the following tools:

| Script | Purpose |
|---|---|
| `make_metadata_from_data.py` | Generate CSVW-SAFE metadata from data |
| `make_dummy_from_metadata.py` | Generate synthetic datasets from metadata |
| `validate_metadata.py` | Validate metadata structure |
| `validate_metadata_shacl.py` | Validate metadata against SHACL constraints |
| `assert_same_structure.py` | Compare original and dummy dataset structures |
| `csvw_to_smartnoise_sql.py` | Export metadata to SmartNoise SQL |
| `csvw_to_opendp_context.py` | Build OpenDP contexts from metadata |

---

## Workflow Overview

The library supports a complete metadata workflow:

1. Generate metadata from a dataset
2. Review and minimize metadata manually
3. Validate metadata
4. Generate dummy datasets
5. Use metadata in DP systems

![Workflow](../images/csvwsafe_workflow_1.png)

Additional integrations are available for OpenDP and SmartNoise SQL:

![Integrations](../images/csvwsafe_workflow_2.png)

---

## Important Safety Notes

The library assists safe data workflows but does not replace governance decisions.

Automatically generated metadata may reveal sensitive information if not reviewed carefully.

Before publishing metadata:

- Review all generated properties
- Remove unnecessary statistics
- Minimize disclosure
- Verify that all released information is public

Metadata should describe safe assumptions about possible datasets, not confidential properties of the observed dataset itself.