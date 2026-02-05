# CSVW Differential Privacy Extension (CSVW-DP) Vocabulary

## Overview

Differential privacy (DP) requires metadata such as:

* Maximum number of rows contributed by a single individual
* Maximum size of any aggregation partition
* Bounds on how many partitions a person can influence
* Maximum number of partitions a person can influence
* Bounds on per-partition contributions
* Constraints preventing overflow or numerical instability during aggregation

These assumptions are essential for meaningful DP guarantees, but the core CSVW vocabulary cannot express them.

**CSVW-DP** extends the [CSV on the Web (CSVW)](https://www.w3.org/TR/tabular-data-model/) vocabulary with a declarative, semantic, DP-aware data modeling system, allowing to:

* Explicitly declare contribution bounds at table, column, partition and multi-column levels
* Model both categorical and continuous grouping keys
* Attach DP constraints to single columns and multi-column groupings
* Interoperate with existing CSVW tooling and DP libraries

See 
- [guidelines and notes](https://github.com/dscc-admin-ch/csvw-dp/blob/main/guidelines.md).
- Example metadata: [Penguin dataset YAML](https://github.com/dscc-admin-ch/csvw-dp/blob/main/penguin_metadata.yaml).

CSVW-DP separates concerns into:

1. **Vocabulary** – What the data and grouping represents
2. **Constraints** – What bounds and assumptions are allowed

---

## 1. Vocabulary

### Namespace & Definitions

* **Default namespace:** `https://w3id.org/csvw-dp#`
* **Vocabulary definitions:** `csvw-dp-vocab.ttl`
* **JSON-LD context:** `csvw-dp-context.jsonld`


Motivation: This vocabulary is designed to align with the requirements of common DP libraries.
See [dp_libraries.md](https://github.com/dscc-admin-ch/csvw-dp/blob/main/dp_libraries.md) for an overview of the main DP libraries and the parameters they use.

The minimum required fields to to fully parameterize worst case sensitivity for DP on a CSVW table:
- `dp:partitionLength` at the table level,
- `dp:maxPartitionContribution` at the table level,
- bounds (min, max) of continuous columns.



### 1.1 Core Classes

| Class              | Subclass of      | Purpose / Notes                                                                                                                                       |
| ------------------ | ---------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------- |
| `dp:DPBounded`     | –                | Abstract class: anything with DP bounds (`dp:maxPartitionLength`, `dp:maxPartitionContribution`, `dp:maxInfluencedPartitions`, `dp:partitionLength`). |
| `csvw:Table`       | `dp:DPBounded`   | Represents a CSV table. Table-level DP properties (`dp:maxTableLength`, `dp:maxContributions`) are attached here.                                     |
| `csvw:TableSchema` | –                | Defines table structure: columns, primary keys, foreign keys, etc.                                                                                    |
| `dp:GroupingKey`   | `dp:DPBounded`   | Abstract class defining a key space for DP aggregation (single column or multi-column).                                                               |
| `csvw:Column`      | `dp:GroupingKey` | Column-level grouping key. Only columns used as grouping keys are DP-bounded.                                                                         |
| `dp:ColumnGroup`   | `dp:GroupingKey` | Multi-column grouping key; lists constituent `csvw:Column`s; can declare public partitions and DP bounds.                                             |
| `dp:PartitionKey`  | `dp:DPBounded`   | Represents one allowed partition. Can be categorical, numeric, or multi-column. May reference structural metadata (`dp:partitionBindings`).           |


#### DP vs Structural Metadata

CSVW-DP distinguishes between:

**DP-relevant metadata**
Metadata that affects the privacy guarantees or accounting.

**Structural metadata**
Metadata that only describes how grouping keys and partitions are constructed.

This distinction is semantic only; both may appear on the same object.

| Property                      | Category        | Reason                                                    |
| ----------------------------- | --------------- | --------------------------------------------------------- |
| `dp:maxPartitionLength`       | DP              | Bounds sensitivity                                        |
| `dp:maxPartitionContribution` | DP              | l∞ bound                                                  |
| `dp:maxInfluencedPartitions`  | DP              | l₀ bound                                                  |
| `dp:partitionLength`          | DP              | Observed sensitivity                                      |
| `dp:maxNumPartitions`         | DP              | Limits group explosion                                    |
| `dp:publicPartitions`         | **DP-relevant** | Declares grouping universe; avoids data-dependent release |
| `dp:partitionBindings`        | **Structural**  | Describes which column values define a partition          |



### 1.2 Column-Level Properties
Applied to `csvw:Column` only:

| Term                    | Type         | Meaning                                          |
| ----------------------- | ------------ | ------------------------------------------------ |
| `dp:privacyId`          | boolean      | True if column identifies individuals/units.     |
| `dp:nullableProportion` | decimal 0–1  | Fraction of null values (approximate modeling).  |

Standard CSVW terms properties (`datatype`, `format`, `minimum`, `maximum`, `required`, `default`) are re-used as is.
`dp:nullableProportion` is optional and mostly for modeling. (TODO: see how to handle).



### 1.3 Multi Column Properties
Applied to `dp:ColumnGroup` only:

| Property               | Meaning                                                |
| ---------------------- | ------------------------------------------------------ |
| `dp:columns`           | Constituent columns                                    |




### 1.4 Differential Privacy Bounds

Properties of `dp:DPBounded` and their applicability

| Property                      | Meaning                                   | Applicability                                      |
| ----------------------------- | ----------------------------------------- | -------------------------------------------------- |
| `dp:maxPartitionLength`       | Maximum number of rows in scope           | All DP-bounded entities                            |
| `dp:maxPartitionContribution` | Maximum rows per person per partition     | All DP-bounded entities                            |
| `dp:maxInfluencedPartitions`  | Maximum partitions a person can influence | Columns, ColumnGroups, Partitions (tables fixed to 1) |
| `dp:partitionLength`          | Observed number of rows (if known)        | All DP-bounded entities                            |
| `dp:maxNumPartitions`         | Maximum distinct groups                   | Columns, ColumnGroups                              |


#### 1.4.1 Table-Level Mapping 

`csvw:Table` is `dp:DPBounded`: the table is treated as a single large partition.
`dp:partitionLength` is a compulsory field at the table level.
`dp:maxInfluencedPartitions = 1` (one large partition).

> **Reference:** [Casacuberta et al., 2022](https://dl.acm.org/doi/pdf/10.1145/3548606.3560708)
```
{
    "@type": "csvw:Table",
    "dp:maxPartitionLength": 500,
    "dp:partitionLength": 500,
    "dp:maxPartitionContribution": 2,
}
```

#### 1.4.2 Column Level
Columns used as grouping keys inherit all `dp:DPBounded` properties.
```
{
    "@type": "csvw:Column",
    "name": "island",
    "dp:maxNumPartitions": 3,
    "dp:maxPartitionLength": 100,
    "dp:maxInfluencedPartitions": 1,
    "dp:maxPartitionContribution": 2,
}
```

#### 1.4.3 Multi-Column Level
Multiple Columns used as grouping keys inherit all `dp:DPBounded` properties.
```
{
    "@type": "dp:ColumnGroup",
    "dp:columns": ["sex", "island"],
    "dp:maxNumPartitions": 6,
    "dp:maxPartitionLength": 100,
    "dp:maxInfluencedPartitions": 1,
    "dp:maxPartitionContribution": 2,
}
```


#### 1.4.4 Partition Level
Partitions inherit all `dp:DPBounded` properties except `dp:maxNumPartitions`, which is always 1.
A `dp:PartitionKey` inherits DP bounds from its parent `dp:GroupingKey` unless explicitly overridden.
```
{
    "@type": "dp:PartitionKey",
    "dp:partitionValue": "Torgersen",
    "dp:maxPartitionLength": 50,
    "dp:maxInfluencedPartitions": 1,
    "dp:maxPartitionContribution": 2
},
```



### 1.5 Grouping Keys and Public Partitions

Grouping keys (single columns or column groups) may declare `dp:publicPartitions`.
In `dp:ColumnGroup`, partition keys may include optional `dp:partitionBindings` to enumerate explicit combinations for multi-column groups.

| Concept                | Meaning                                             |
| ---------------------- | --------------------------------------------------- |
| `dp:GroupingKey`       | Something you can group by (column or column group) |
| `dp:PartitionKey`      | One allowed group value for a grouping key          |
| `dp:publicPartitions`  | The explicit universe of allowed groups             |
| `dp:partitionBindings` | Optional mapping from grouping columns → values     |

Even if DP bounds are satisfied, exposing `dp:publicPartitions` may contribute to the privacy loss if the data is sensitive.
`dp:partitionBindings` is purely structural; it enumerates the combinations of column values that define a partition. It does not affect DP bounds directly.

The `dp:PartitionKey` has the following structure 

| Property                      | Type             | Meaning                                                                                                                                            |
| ----------------------------- | ---------------- | -------------------------------------------------------------------------------------------------------------------------------------------------- |
| `dp:partitionValue`           | literal          | Categorical partition value (optional if numeric)                                                                                                  |
| `dp:lowerBound`               | decimal          | Lower bound for numeric partitions (inclusive by default)                                                                                          |
| `dp:upperBound`               | decimal          | Upper bound for numeric partitions (exclusive by default)                                                                                          |
| `dp:lowerInclusive`           | boolean          | Whether `lowerBound` is inclusive (default: true)                                                                                                  |
| `dp:upperInclusive`           | boolean          | Whether `upperBound` is inclusive (default: false)                                                                                                 |
| `dp:partitionLabel`           | string           | Optional human-readable label                                                                                                                      |
| `dp:partitionBindings`        | object / map     | Optional mapping from grouping columns to values (subset of Cartesian product). Useful for multi-column groups. |


#### 1.6.1 Column Level
Categorical
```
{
  "name": "sex",
  "datatype": "string",
  "required": false,
  "dp:privacyId": false,
  "dp:nullableProportion": 0.2,
  "dp:publicPartitions": [{
        "@type": "dp:PartitionKey", 
        "dp:partitionValue": "MALE",
        "dp:maxPartitionLength": 50,
        "dp:maxInfluencedPartitions": 1,
        "dp:maxPartitionContribution": 2
    },{
        "@type": "dp:PartitionKey", 
        "dp:partitionValue": "FEMALE",
        "dp:maxPartitionLength": 50,
        "dp:maxInfluencedPartitions": 1,
        "dp:maxPartitionContribution": 2
    }]
}
```

Continuous
```
{
    "name": "flipper_length_mm",
    "datatype": "double",
    "required": false,
    "dp:privacyId": false,
    "dp:nullableProportion": 0.05,
    "dp:publicPartitions": [{
        "@type": "dp:PartitionKey",
        "dp:lowerBound": 150.0,
        "dp:upperBound": 200.0,
        "dp:lowerInclusive": true,
        "dp:upperInclusive": false,
        "dp:maxPartitionLength": 100,
        "dp:maxInfluencedPartitions": 1,
        "dp:maxPartitionContribution": 1
    },{
        "@type": "dp:PartitionKey",
        "dp:lowerBound": 200.0,
        "dp:upperBound": 250.0,
        "dp:lowerInclusive": true,
        "dp:upperInclusive": true,
        "dp:maxPartitionLength": 100,
        "dp:maxInfluencedPartitions": 1,
        "dp:maxPartitionContribution": 1
    }]
},
```

#### 1.6.2 Multi-Column Level

Here, `dp:partitionBindings` is used to enumerate explicit combinations (subset of Cartesian product).
Categorical
```
{
    "@type": "dp:ColumnGroup",
    "dp:columns": ["sex", "island"],
    "dp:publicPartitions": [
      {
        "@type": "dp:PartitionKey",
        "dp:partitionBindings": {
            "sex" : "MALE", 
            "island": "Torgersen"
        },
        "dp:maxPartitionLength": 50,
        "dp:maxInfluencedPartitions": 1,
        "dp:maxPartitionContribution": 2
      },
      {
        "@type": "dp:PartitionKey",
        "dp:partitionBindings": {
            "sex" : "FEMALE", 
            "island": "Torgersen"
        },
        "dp:maxPartitionLength": 50,
        "dp:maxInfluencedPartitions": 1,
        "dp:maxPartitionContribution": 2
      }
    ]
}
```
Continuous
```
{
    "@type": "dp:ColumnGroup",
    "dp:columns": ["sex", "flipper_length_mm"],
    "dp:publicPartitions": [
      {
        "@type": "dp:PartitionKey",
        "dp:partitionBindings": {
            "sex" : "MALE", 
            "flipper_length_mm": {
                "dp:lowerBound": 150.0,
                "dp:upperBound": 200.0
            }
        },
        "dp:maxPartitionLength": 50,
        "dp:maxInfluencedPartitions": 1,
        "dp:maxPartitionContribution": 2
      },
      {
        "@type": "dp:PartitionKey",
        "dp:partitionBindings": {
            "sex" : "FEMALE", 
            "flipper_length_mm": {
                "dp:lowerBound": 150.0,
                "dp:upperBound": 200.0
            }
        },
        "dp:maxPartitionLength": 50,
        "dp:maxInfluencedPartitions": 1,
        "dp:maxPartitionContribution": 2
      }
    ]
}
```

### 1.7 Relation to DP Metrics

| DP Metric  | Meaning                               | CSVW-DP Term                  |
| ---------- | ------------------------------------- | ----------------------------- |
| $l_0$      | Affected partitions per person        | `dp:maxInfluencedPartitions`  |
| $l_\infty$ | Max contribution within one partition | `dp:maxPartitionContribution` |
| $l_1$      | Total changed rows                    | Derived: `l₀ × l∞`            |

### 1.8 Visual Overview

```
csvw:Table ⊂ dp:DPBounded
 ├─ dp:maxPartitionLength
 ├─ dp:partitionLength
 ├─ dp:maxPartitionContribution
 │
 ├─ csvw:tableSchema → csvw:TableSchema
 │    └─ csvw:Column ⊂ dp:GroupingKey ⊂ dp:DPBounded
 │         ├─ CSVW schema (datatype, required, default, etc.)
 │         ├─ dp:publicPartitions (DP-relevant)
 │         │    └─ optional dp:partitionBindings (structural)
 │         └─ DP bounds
 │
 └─ dp:ColumnGroup ⊂ dp:GroupingKey ⊂ dp:DPBounded
      ├─ dp:columns → rdf:List(csvw:Column)
      ├─ dp:publicPartitions (DP-relevant)
      │    └─ optional dp:partitionBindings (structural)
      └─ DP bounds
```

> **Full View:** [README_details.md](https://github.com/dscc-admin-ch/csvw-dp/blob/main/README_details.md)

---

## 2. Constraints

### 2.1 Worst-Case Bounds for Multi-Column Grouping

| Property                      | Worst-case derivation          |
| ----------------------------- | ------------------------------ |
| `dp:maxPartitionLength`       | `min` of constituent bounds    |
| `dp:maxNumPartitions`         | ≤ product of per-column maxima |
| `dp:maxInfluencedPartitions`  | `min` of known bounds          |
| `dp:maxPartitionContribution` | `min` of known bounds          |
| `dp:publicPartitions`         | Cartesian product              |

Public partitions are only allowed if all grouped columns declare them (before: cartesian product?).

> **Example:** [README_details.md](https://github.com/dscc-admin-ch/csvw-dp/blob/main/README_details.md)

### 2.2 Column-Level Constraints

| Rule                                   | Meaning                             |
| -------------------------------------- | ----------------------------------- |
| `required=true → nullableProportion=0` | Required columns cannot be nullable |
| `privacyId=true` forbids DP bounds     | Identifiers cannot be aggregated    |
| Partition values must match datatype   | Type safety                         |


### 2.3 Multi-Column Group Constraints

| Rule                                                              | Meaning                          |
| ----------------------------------------------------------------- | -------------------------------- |
| No `dp:privacyId` columns in group                                | Privacy ID cannot participate    |
| If any column lacks `dp:publicPartitions`, group must not declare | Prevent unsafe assumptions       |
| If any column lacks `dp:maxNumPartitions`, group must not declare | Avoid underestimating partitions |

### 2.4 Table-Level Consistency Rules

On any partition, column, columngroup, cannot have more rows than initial table.

| Property                      | Constraint              |
| ----------------------------- | ----------------------- |
| `dp:tableLength`              | = `dp:maxTableLength`   |
| `dp:maxPartitionLength`       | ≤ `dp:maxTableLength`   |
| `dp:maxInfluencedPartitions`  | ≤ `dp:maxContributions` |
| `dp:maxPartitionContribution` | ≤ `dp:maxContributions` |

> SHACL enforcement: [`metadata_constraints.ttl`](https://github.com/dscc-admin-ch/csvw-dp/blob/main/csvw-dp-constaints.ttl)

---

## 3. Summary of Files

| File                          | Purpose                             |
| ----------------------------- | ----------------------------------- |
| `README.md`                   | Description, Motivation             |
| `csvw-dp-vocab.ttl`           | Vocabulary definition (OWL + RDFS)  |
| `csvw-dp-context.jsonld`      | JSON-LD context                     |
| `csvw-dp-constraints.ttl`     | SHACL validation rules              |
| `penguin_metadata.json`       | Example metadata                    |
| `dp_libraries.md`             | Mapping to DP libraries             |
| `validate_metadata.py`        | Metadata validator                  |
| `make_metadata_from_data.py`  | Infer baseline CSVW metadata        |
| `make_dummy_from_metadata.py` | Dummy data generator                |

`make_metadata_from_data.py` and `make_dummy_from_metadata.py` still need DerivedColumn, GroupColumn logic (maybe).
