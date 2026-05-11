# Integrations

CSVW-SAFE metadata can be integrated with Differential Privacy libraries and systems.

Currently supported integrations include:

- SmartNoise SQL
- OpenDP

---

# SmartNoise SQL Integration

The `csvw_to_smartnoise_sql.py` utility converts CSVW-SAFE metadata into SmartNoise SQL YAML configuration.

---

## Supported Mappings

| CSVW-SAFE | SmartNoise SQL |
|---|---|
| `maxContributions` | `max_ids` |
| `minimum` | `lower` |
| `maximum` | `upper` |
| `privacyId` | `private_id` |

---

## Usage

```bash
python csvw_to_smartnoise_sql.py \
  --input metadata.json \
  --output snsql_metadata.yaml
```

## Example output

```yaml
table:
  max_ids: 3

  age:
    type: int
    lower: 0
    upper: 100
```


# OpenDP Integration
The `csvw_to_opendp_context.py` utility creates OpenDP contexts from metadata and datasets.

## Usage
```bash
import polars as pl

from csvw_safe.csvw_to_opendp_context import (
    csvw_to_opendp_context,
)

data = pl.scan_csv("data.csv")

context = csvw_to_opendp_context(
    csvw_meta=metadata,
    data=data,
    epsilon=1.0,
)
```


## Design Goal

CSVW-SAFE aims to provide a portable metadata layer between datasets and Differential Privacy systems.

The metadata should remain implementation-independent while still enabling automated DP workflows.