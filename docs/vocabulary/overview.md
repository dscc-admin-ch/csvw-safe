# CSVW-SAFE Overview

CSVW-SAFE extends the W3C [CSV on the Web (CSVW)](https://www.w3.org/TR/tabular-data-model/) standard with privacy-safe metadata for:

- Differential Privacy (DP)
- Dummy data generation
- Structural dataset modeling
- Public partition definitions
- Safe schema publication

![Overview](../images/csvw-safe_structure.png)

CSVW-SAFE allows organizations to publish assumptions and guarantees about datasets without exposing sensitive underlying records.

These assumptions may include:

- dataset schema
- nullable proportions
- public categorical domains
- grouping partitions
- contribution bounds for DP
- logical dependencies between columns

!!! danger "Warning"

    Some assumptions may themselves leak sensitive information.
    Metadata must always be manually reviewed before publication.


## Main Concepts

CSVW-SAFE extends CSVW with:

| Concept | Purpose |
|---|---|
| Structural modeling | Describe possible datasets |
| Dummy modeling | Generate realistic fake datasets |
| DP contribution bounds | Calibrate differential privacy |
| Public partitions | Define safe grouping assumptions |
| Validation | Ensure metadata consistency |

## Related Components

| File | Purpose |
|---|---|
| `csvw-safe-vocab.ttl` | RDF vocabulary |
| `csvw-safe-context.jsonld` | JSON-LD context |
| `csvw-safe-constraints.ttl` | SHACL validation |
| `csvw-safe-library` | Python tooling |
