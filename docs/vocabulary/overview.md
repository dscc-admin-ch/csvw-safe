# CSVW-EO Overview

CSVW-EO extends the W3C [CSV on the Web (CSVW)](https://www.w3.org/TR/tabular-data-model/) standard with privacy-safe metadata for:

- Differential Privacy (DP)
- Dummy data generation
- Structural dataset modeling
- Public partition definitions
- Safe schema publication

![Overview](../images/csvw-eo_structure.png)

CSVW-EO allows organizations to publish assumptions and guarantees about datasets without exposing sensitive underlying records.

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

CSVW-EO extends CSVW with:

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
| `csvw-eo-vocab.ttl` | RDF vocabulary |
| `csvw-eo-context.jsonld` | JSON-LD context |
| `csvw-eo-constraints.ttl` | SHACL validation |
| `csvw-eo-library` | Python tooling |
