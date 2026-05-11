# Differential Privacy Contributions

CSVW-SAFE defines metadata for Differential Privacy (DP) calibration.

These properties describe worst-case assumptions about how privacy units may contribute to datasets.

## Privacy Unit

A privacy unit identifies the entity protected by DP.

Examples:

- patient
- user
- household
- hospital

Property:

| Property | Meaning |
|---|---|
| `privacyUnit` | Privacy identifier column |

## DP Contribution Properties

| Property | Meaning |
|---|---|
| `maxContributions` | Maximum rows contributed |
| `maxGroupsPerUnit` | Maximum groups affected |
| `maxLength` | Maximum dataset/partition size |
| `publicLength` | Exact public size |

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

## Contribution Levels

CSVW-SAFE supports multiple granularity levels:

| Level | Description |
|---|---|
| `table` | Table only |
| `table_with_keys` | Table + public keys |
| `column` | Per-column contributions |
| `partition` | Fine-grained partitions |

More detailed contribution assumptions may increase privacy risk.