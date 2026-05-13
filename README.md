# CSVW-SAFE

CSVW-SAFE extends the W3C CSV on the Web (CSVW) standard with
privacy-safe metadata for:

- Differential Privacy contribution modeling
- Structural dataset assumptions
- Dummy data generation
- Public partition/key definitions
- Validation workflows

Documentation:
https://dscc-admin-ch.github.io/csvw-safe-docs/

---

## Why CSVW-SAFE?

Many datasets cannot be shared directly due to privacy or governance
constraints.

CSVW-SAFE allows publishing safe assumptions about datasets without
sharing the underlying sensitive data.

Examples include:

- schema information
- nullable proportions
- public partitions
- contribution bounds for DP
- logical dependencies

![Overview](images/csvw-safe_structure.png)

---

## Repository Structure

| Component | Description |
|---|---|
| `csvw-safe-library/` | Python library |
| `docs/` | MkDocs documentation |
| `csvw-safe-vocab.ttl` | RDF vocabulary |
| `csvw-safe-constraints.ttl` | SHACL validation rules |

---

## Installation

```bash
pip install csvw-safe
```

## Quick Example

```bash
from csvw_safe.make_metadata_from_data import make_metadata_from_data
from csvw_safe.make_dummy_from_metadata import make_dummy_from_metadata

metadata = make_metadata_from_data(
    df,
    privacy_unit="user_id",
)
dummy_df = make_dummy_from_metadata(metadata)
```

## Documentation

| Section        | Link                                                                                                                     |
| -------------- | ------------------------------------------------------------------------------------------------------------------------ |
| Vocabulary     | [https://dscc-admin-ch.github.io/csvw-safe-docs/vocabulary/overview/](https://dscc-admin-ch.github.io/csvw-safe-docs/vocabulary/overview/) |
| Python Library | [https://dscc-admin-ch.github.io/csvw-safe-docs/library/overview/](https://dscc-admin-ch.github.io/csvw-safe-docs/library/overview/)       |
| API Reference  | [https://dscc-admin-ch.github.io/csvw-safe-docs/api/](https://dscc-admin-ch.github.io/csvw-safe-docs/api/)               |
