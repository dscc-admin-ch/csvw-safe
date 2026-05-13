# Differential Privacy Contributions

CSVW-EO defines metadata for Differential Privacy (DP) calibration.

These properties describe worst-case assumptions about how privacy units may contribute to datasets.

!!! danger "Warning"

    Contribution assumptions must only describe public, non-sensitive information.
    More detailed contribution metadata may increase privacy leakage risk and should always be manually reviewed before publication.

## Privacy Unit

A privacy unit identifies the entity protected by DP.

Examples:

- patient
- user
- household
- hospital

Two datasets are considered neighbouring datasets if and only if all rows associated with one privacy unit are added or removed.

CSVW-EO currently assumes a single privacy unit per dataset.

| Property      | Type    | Meaning                                   | Level  |
| ------------- | ------- | ----------------------------------------- | ------ |
| `privacyUnit` | string  | Name of the privacy identifier column     | Table  |
| `privacyId`   | boolean | Whether a column identifies privacy units | Column |

### Example

```json
{
  "@type": "csvw:Table",
  "name": "hotpitalisations",
  "privacyUnit": "patient_id",
  "tableSchema": {
    "columns": [
        {
            "@type": "csvw:Column",
            "name": "patient_id",
            "privacyId": true,
            "datatype": "int"
        },
        {
            "@type": "csvw:Column",
            "name": "diagnostic",
            "privacyId": false,
            "datatype": "string"
        },
    ]
  },
  "additionalInformation": []
}
```


## DP Contribution Properties

| Property              | Meaning                                                |  Table  | Partition | Column / ColumnGroup |
| --------------------- | ------------------------------------------------------ | :-----: | :-------: | :------------------: |
| `maxContributions`    | Maximum rows contributed by one privacy unit (`l∞`)    | Yes (1) |  Yes (3)  |          No          |
| `maxLength`           | Maximum dataset or partition size                      | Yes (2) |  Yes (4)  |          No          |
| `publicLength`        | Exact public size                                      |   Yes   |    Yes    |          No          |
| `maxGroupsPerUnit`    | Maximum groups affected by one privacy unit (`l0`)     |    No   |     No    |          Yes         |
| `invariantPublicKeys` | Whether keys are public independently of privacy units |    No   |     No    |          Yes         |


## Required DP Fields
Some properties are mandatory for DP calibration depending on the query type.

| Requirement | Required For                      | Meaning                          |
| ----------- | --------------------------------- | -------------------------------- |
| Yes (1)     | Table-level queries               | Maximum contributions in dataset |
| Yes (2)     | Table-level queries except counts | Maximum dataset size             |
| Yes (3)     | `GROUP BY` queries                | Maximum contributions per group  |
| Yes (4)     | `GROUP BY` queries except counts  | Maximum group size               |

Other properties are optional but may improve utility and reduce unnecessary DP noise.

## `maxContributions`

Defines the maximum number of rows contributed by one privacy unit.

At:

- table level → whole dataset
- partition level → one group

## `maxGroupsPerUnit`

Defines how many groups one privacy unit may affect.

Examples:

- one patient may appear in 12 months
- one user may appear in 3 regions

## `maxLength`

Defines theoretical maximum dataset size.

This is useful for:

- DP calibration
- numerical stability
- overflow prevention

## Partition-Level Contributions

Partitions may define finer contribution assumptions.

Example:

- February → 28 contributions
- July → 31 contributions

## Total Influence of a Privacy Unit

The total influence of a privacy unit corresponds to the total number of rows that may be affected when one privacy unit is added or removed from the dataset.

It is defined as:

$$
l_1 = l_0 \cdot l_\infty
$$

where:

- $l_0$ = `maxGroupsPerUnit`  
  (maximum number of groups a privacy unit may affect)

- $l_\infty$ = `maxContributions`  
  (maximum number of rows contributed within one group)

CSVW-EO does not define $l_1$ as a separate metadata property because its interpretation depends on the query structure and grouping context.

## Contribution Levels

CSVW-EO supports multiple granularity levels for contribution metadata.

| Level             | Description                          | Privacy Risk |
| ----------------- | ------------------------------------ | ------------ |
| `table`           | Table-level contributions only       | Lowest       |
| `table_with_keys` | Table-level + public keys            | Medium       |
| `column`          | Per-column/group contributions       | Medium       |
| `partition`       | Fine-grained partition contributions | Highest      |


More detailed contribution assumptions may increase privacy risk.

General recommendation:

- start with `table`
- only increase granularity if required
- always minimise disclosed metadata

### Example
Example of partition-level DP contributions:
```json
{
  "@type": "Partition",
  "predicate": {
    "partitionValue": "Adelie"
  },
  "maxLength": 200,
  "maxGroupsPerUnit": 3,
  "maxContributions": 1
}
```

Interpretation:

- at most 200 rows in this partition
- one privacy unit may affect at most 3 groups
- one privacy unit contributes at most 1 row inside this partition