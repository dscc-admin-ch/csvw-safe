# CSVW-EO Classes

CSVW-EO defines four main classes.

| Class | Purpose |
|---|---|
| `csvw:Table` | Dataset-level metadata |
| `csvw:Column` | Single-column metadata |
| `csvw-eo:ColumnGroup` | Multi-column grouping metadata |
| `csvw-eo:Partition` | Public partition definitions |

## Table

A `csvw:Table` represents the global dataset.

It contains:

- table schema
- privacy unit definition
- global DP bounds

Example:

```json
{
  "@type": "csvw:Table",
  "name": "penguins",
  "privacyUnit":"penguin_id",
  "maxContributions": 3,
  "maxLength": 1000,
  "tableSchema": {
    "columns": []
  },
  "additionalInformation": []
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
  "datatype": "string",
  "required": true,
  "privacyId": false,
  "keyValues":["Adelie", "Chinstrap", "Gentoo"],
  "exhaustiveKeys": true,
}
```

## ColumnGroup

A `csvw-eo:ColumnGroup` describes grouping assumptions on multiple columns simultaneously.

Typical use cases:

- (year, month)
- (country, city)

It may contain:

- public partitions
- grouped contribution bounds
- grouped public keys

```json
"additionalInformation":[
  {
      "@type":"ColumnGroup",
      "columnsInGroup":[
        "species",
        "island"
      ],
      "partitions":[
        {
            "@type":"Partition",
            "predicate":{
              "species":{
                  "partitionValue":"Adelie"
              },
              "island":{
                  "partitionValue":"Biscoe"
              }
            },
            "maxLength":50, # of this partition
            "maxGroupsPerUnit":3,
            "maxContributions":1
        }
      ],
      "exhaustivePartitions": false,
      "maxLength":200, # of any partition of this ColumnGroup
  }
]
```

## Partition

A `csvw-eo:Partition` defines a region of a value domain.

Examples:

- one category
- one month
- one numerical interval

Partitions are indentified by predicates and may contain:

- contribution bounds
- public lengths

A `csvw:Column` or a `csvw-eo:ColumnGroup` can have a list of partitions in the field `partition`. If they are exhaustive then, `exhaustivePartitions` may be True.

Example:

Categorical column
```json
{
  "@type": "csvw-eo:Partition",
  "csvw-eo:predicate": {
    "partitionValue": "Biscoe"
  },
  "maxLength": 300,
  "maxGroupsPerUnit": 3,
  "maxContributions": 1
}
```

Continuous column
```json
{
  "@type": "csvw-eo:Partition",
  "csvw-eo:predicate": {
    "lowerBound": 100,
    "upperBound": 200,
  },
  "maxLength": 300,
  "maxGroupsPerUnit": 3,
  "maxContributions": 1
}
```

## JSON-LD Structure

CSVW-EO extends standard CSVW JSON-LD structures with additional properties and objects.
This image presents the base `csvw` json-ld structure on the left and the extended `csvw-eo`.

![Overview](../images/csvw-eo_structure.png)