# Dummy Modeling Properties

CSVW-SAFE defines properties useful for generating realistic dummy datasets.

These properties improve synthetic data quality while remaining compatible with differential privacy workflows.

## Nullable Proportion

| Property | Meaning |
|---|---|
| `nullableProportion` | Approximate fraction of null values |

Example:

```json
{
  "nullableProportion": 0.15
}
```

## Dependencies

Dependencies describe relationships between columns.

| Property         | Meaning            |
| ---------------- | ------------------ |
| `dependsOn`      | Source column      |
| `dependencyType` | Type of dependency |
| `valueMap`       | Mapping definition |

### Dependency Types

#### `bigger`
Indicates that one column is always greater than another. Only useful for columns whose bounds overlap.

Example:

- date_treatment_1 > date_treatment_2
- date_of_death > date_of_birth


#### `mapping`

Defines deterministic or constrained mappings.

Example:

```json
{
  "dependencyType": "mapping",
  "valueMap": {
    "child": false,
    "adult": true
  }
}
```

#### `fixedPerEntity`

Indicates values remain constant for the same privacy unit.

Example:

- birth date per patient
- height per (adult) person


## Public Keys

CSVW-SAFE can define known public domains.

| Property         | Meaning                     |
| ---------------- | --------------------------- |
| `publicKeys`     | Known public values         |
| `exhaustiveKeys` | Whether keys are exhaustive |

Example:

```json
{
  "publicKeys": ["January", "February"]
}
```

## Partitions

Partitions describe public regions of the domain.

Examples:

- categories
- numerical intervals
- grouped partitions

Partitions may be:

- exhaustive
- overlapping
- disjoint