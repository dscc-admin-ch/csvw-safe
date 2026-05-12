# Dummy Modeling Properties

CSVW-SAFE defines properties useful for generating realistic dummy datasets.

These properties improve dummy data quality while remaining compatible with differential privacy workflows.

!!! danger "Warning"

    If they disclose private information, they should not be added.


## Nullable Proportion

| Property | Meaning |
|---|---|
| `nullableProportion` | Approximate fraction of null values |
| `maxNumPartitions` | Maximum number of partition (keys) |


Example:

```json
{
  "nullableProportion": 0.15,
  "maxNumPartitions": 5,
  "publicKeys": ["January", "February"],
  "exhaustiveKeys": false
}
```
These column will be generated with arounf 15% of null values and take values in ["January", "February", "a", "b", "c"]. The public keys and then random keys to have the same number of partitions as `maxNumPartitions`.

Note: `nullableProportion` may be approximate. 

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

Another example:

```json
{
  "dependencyType": "mapping",
  "valueMap": {
    "medical": ["doctor", "nurse"],
    "engineer": ["civil", "mechanical"]
  }
}
```

#### `fixedPerEntity`

Indicates values remain constant for the same privacy unit.

Example:

- birth date per patient
- country of birth per individual
- height per (adult) person


## Public Keys

CSVW-SAFE can define known public domains.

| Property         | Meaning                     |
| ---------------- | --------------------------- |
| `publicKeys`     | List of public values       |
| `exhaustiveKeys` | Whether keys are exhaustive |

Example:

```json
{
  "publicKeys": ["January", "February"],
  "exhaustiveKeys": false
}
```

```json
{
  "keyValues": [
    "January",
    "February",
    "March",
    "April",
    "May",
    "June",
    "July",
    "August",
    "September",
    "October",
    "November",
    "December"
  ],
  "exhaustiveKeys": true
}
```
If `exhaustiveKeys=true`, all possible keys are publicly listed.

## Partitions

Partitions describe public regions of the domain.

| Property         | Meaning                     |
| ---------------- | --------------------------- |
| `partition`      | List of `csvw-safe:Partition` objects |
| `exhaustivePartitions` | Whether partitions fully cover the domain |

Examples:

- categories
- numerical intervals
- grouped partitions

Partitions may be:

- exhaustive
- overlapping
- disjoint

Both `csvw:Column` and `csvw:ColumnGroup` may define partitions.

### Example: Column Partitions

Example partitioning on the `species` column:

```json
{
  "name": "species",
  "datatype": "string",
  "partitions": [
    {
      "@type": "Partition",
      "predicate": {
        "partitionValue": "Adelie"
      },
      "maxLength": 152
    },
    {
      "@type": "Partition",
      "predicate": {
        "partitionValue": "Gentoo"
      },
      "maxLength": 124
    }
  ],
  "keyValues": [
    "Adelie",
    "Gentoo"
  ],
  "exhaustiveKeys": true,
  "maxNumPartitions": 2
}
```


### Example: Column Group Partitions

Example partitioning on the `(species, island)` column:

```json
{
  "@type": "ColumnGroup",
  "columnsInGroup": [
    "species",
    "island"
  ],
  "partitions": [
    {
      "@type": "Partition",
      "predicate": {
        "species": {
          "partitionValue": "Adelie"
        },
        "island": {
          "partitionValue": "Dream"
        }
      },
      "maxLength": 100
    }
  ],
  "maxNumPartitions": 5
}
```
Column groups describe partitions resulting from grouping on multiple columns simultaneously.