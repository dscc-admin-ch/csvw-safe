# CSVW-SAFE Classes

CSVW-SAFE defines four main classes.

| Class | Purpose |
|---|---|
| `csvw:Table` | Dataset-level metadata |
| `csvw:Column` | Single-column metadata |
| `csvw-safe:ColumnGroup` | Multi-column grouping metadata |
| `csvw-safe:Partition` | Public partition definitions |

## Table

A `csvw:Table` represents the global dataset.

It contains:

- table schema
- privacy unit definition
- global DP bounds
- additional grouping metadata

Example:

```json
{
  "@type": "csvw:Table",
  "name": "penguins"
}
```

## Column

A `csvw:Column` defines metadata for one column.

It may contain:

- datatype
- nullable proportion
- public keys
- grouping contribution assumptions
- dependencies

Example:
```json
{
  "@type": "csvw:Column",
  "name": "species",
  "datatype": "string"
}
```

## ColumnGroup

A `csvw-safe:ColumnGroup` describes grouping assumptions on multiple columns simultaneously.

Typical use cases:

- (year, month)
- (country, city)

It may contain:

- public partitions
- grouped contribution bounds
- grouped public keys

## Partition

A `csvw-safe:Partition` defines a region of a value domain.

Examples:

- one category
- one month
- one numerical interval

Partitions are indentified by predicates and may contain:

- contribution bounds
- public lengths

Example:
```json
{
  "@type": "csvw-safe:Partition",
  "csvw-safe:predicate": {
    "month": "January"
  }
}
```

## JSON-LD Structure

CSVW-SAFE extends standard CSVW JSON-LD structures with additional properties and objects.
This image presents the base `csvw` json-ld structure on the left and the extended `csvw-safe`.
![Overview](images/csvw-safe_structure.png)