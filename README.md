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
| `csvw:Table`       | `dp:DPBounded`   | Represents a CSV table. Table-level DP properties.                                     |
| `csvw:TableSchema` | –                | Defines table structure: columns, primary keys, foreign keys, etc.                                                                                    |
| `dp:GroupingKey`   | `dp:DPBounded`   | Abstract class defining a key space for DP aggregation (single column or multi-column).                                                               |
| `csvw:Column`      | `dp:GroupingKey` | Column-level grouping key. Only columns used as grouping keys are DP-bounded.                                                                         |
| `dp:ColumnGroup`   | `dp:GroupingKey` | Multi-column grouping key; lists constituent `csvw:Column`s; can declare public partitions and DP bounds.                                             |
| `dp:PartitionKey`  | `dp:DPBounded`   | Represents one allowed partition. Can be categorical, numeric, or multi-column. May reference structural metadata (`dp:components`).           |


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
| `dp:maxPartitionLength`       | Maximum number of rows in scope           | All dp:DPBounded entities                            |
| `dp:maxPartitionContribution` | Maximum rows per person per partition     | All dp:DPBounded entities                            |
| `dp:maxInfluencedPartitions`  | Maximum partitions a person can influence | All dp:GroupingKey entities (1 for table and partitions) |
| `dp:partitionLength`          | Observed number of rows (if known)        | All dp:DPBounded entities                            |
| `dp:maxNumPartitions`         | Maximum distinct groups                   | All dp:GroupingKey entities (1 for table and partitions) |

`dp:maxNumPartitions` is a structural upper bound on the size of the grouping universe.
It does not directly affect sensitivity unless combined with `dp:maxInfluencedPartitions`.

#### 1.4.1 Table-Level Mapping 

`csvw:Table` is `dp:DPBounded`: the table is treated as a single large partition.
`dp:partitionLength` must be provided at the table level.
`dp:maxInfluencedPartitions = 1` (one large partition).

> **Reference:** [Casacuberta et al., 2022](https://dl.acm.org/doi/pdf/10.1145/3548606.3560708)
```
{
    "@type": "csvw:Table",
    "dp:maxPartitionLength": 500,
    "dp:partitionLength": 500,
    "dp:maxPartitionContribution": 2,
    "dp:maxNumPartitions": 3
}
```

#### 1.4.2 Column Level
Columns used as grouping keys inherit all `dp:DPBounded` properties.
```
{
    "@type": "csvw:Column",
    "name": "island",
    "dp:maxPartitionLength": 100,
    "dp:maxInfluencedPartitions": 1,
    "dp:maxPartitionContribution": 2,
    "dp:maxNumPartitions": 3
}
```

#### 1.4.3 Multi-Column Level
Multiple Columns used as grouping keys inherit all `dp:DPBounded` properties.
```
{
    "@type": "dp:ColumnGroup",
    "dp:columns": ["sex", "island"],
    "dp:maxPartitionLength": 100,
    "dp:maxInfluencedPartitions": 1,
    "dp:maxPartitionContribution": 2,
    "dp:maxNumPartitions": 6
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
    "dp:maxPartitionContribution": 2,
    "dp:maxNumPartitions": 1
},
```


### 1.5 Grouping Keys and Public Partitions

Grouping keys (single columns or column groups) may declare two kinds of metadata:

**DP-relevant metadata**
Metadata that affects privacy guarantees or accounting:
- contribution bounds
- partition size bounds
- influenced-partition bounds

**Structural metadata**
Metadata that describes how partitions are constructed, but does not directly affect DP bounds:
- `dp:maxNumPartitions` — maximum number of distinct groups
- `dp:publicPartitions` — the explicit universe of allowed partitions
- `dp:components` — the internal structure of a composite partition

Declaring public partitions may itself reveal information if the partition values are not already public.
If partition values are declared as metadata, they are assumed to be public and do not consume privacy budget.
As with all DP-related metadata, values should only be declared when safe.

#### Core Concept
| Concept               | Meaning                                             |
| --------------------- | --------------------------------------------------- |
| `dp:GroupingKey`      | Something you can group by (column or column group) |
| `dp:PartitionKey`     | One allowed group value for a grouping key          |
| `dp:publicPartitions` | Explicit universe of allowed partitions             |
| `dp:maxNumPartitions` | Maximum number of allowed partitions                |
| `dp:components`       | Structural decomposition of a composite partition   |




#### `dp:PartitionKey` Structure 

A `dp:PartitionKey` describes a constraint over a value space.

| Property               | Type                    | Meaning                                                    |
| ---------------------- | ----------------------- | ---------------------------------------------------------- |
| `dp:partitionValue`    | literal                 | Categorical partition value                                |
| `dp:lowerBound`        | decimal                 | Lower bound for numeric partitions                         |
| `dp:upperBound`        | decimal                 | Upper bound for numeric partitions                         |
| `dp:lowerInclusive`    | boolean                 | Whether `lowerBound` is inclusive (default: true)          |
| `dp:upperInclusive`    | boolean                 | Whether `upperBound` is inclusive (default: false)         |
| `dp:partitionLabel`    | string                  | Optional human-readable label                              |
| `dp:components` | map → `dp:PartitionKey` | Mapping from grouping columns to per-column partition keys |


`dp:PartitionKey` is a recursive structure describing a constraint over a value space.
For single-column grouping keys, a partition is defined either by a categorical value (`dp:partitionValue`) or by a numeric interval (`dp:lowerBound` / `dp:upperBound`).
For multi-column grouping keys, a partition is defined by `dp:components`, which maps each grouping column to its own `dp:PartitionKey`.
This recursive structure allows categorical and continuous dimensions to be combined uniformly.

In `dp:PartitionKey`/`dp:components`, each key is a column identifier.
These identifiers MUST refer to columns declared in:
- the enclosing `dp:ColumnGroup`/`dp:columns`, and
- the dataset’s column schema.

The datatype and semantics of the referenced column constrain which fields of `dp:PartitionKey` are valid (e.g. categorical vs numeric).

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

Here, `dp:components` is used to enumerate explicit combinations (subset of Cartesian product).
Categorical
```
{
  "@type": "dp:ColumnGroup",
  "dp:columns": ["sex", "island"],
  "dp:publicPartitions": [
    {
      "@type": "dp:PartitionKey",
      "dp:components": {
        "sex": {
          "@type": "dp:PartitionKey",
          "dp:partitionValue": "MALE"
        },
        "island": {
          "@type": "dp:PartitionKey",
          "dp:partitionValue": "Torgersen"
        }
      },
      "dp:maxPartitionLength": 50,
      "dp:maxInfluencedPartitions": 1,
      "dp:maxPartitionContribution": 2
    },
    {
      "@type": "dp:PartitionKey",
      "dp:components": {
        "sex": {
          "@type": "dp:PartitionKey",
          "dp:partitionValue": "FEMALE"
        },
        "island": {
          "@type": "dp:PartitionKey",
          "dp:partitionValue": "Torgersen"
        }
      },
      "dp:maxPartitionLength": 50,
      "dp:maxInfluencedPartitions": 1,
      "dp:maxPartitionContribution": 2
    }
  ]
}
```
Categorical x Continuous
```
{
  "@type": "dp:ColumnGroup",
  "dp:columns": ["sex", "flipper_length_mm"],
  "dp:publicPartitions": [
    {
      "@type": "dp:PartitionKey",
      "dp:components": {
        "sex": {
          "@type": "dp:PartitionKey",
          "dp:partitionValue": "MALE"
        },
        "flipper_length_mm": {
          "@type": "dp:PartitionKey",
          "dp:lowerBound": 150.0,
          "dp:upperBound": 200.0,
          "dp:lowerInclusive": true,
          "dp:upperInclusive": false
        }
      },
      "dp:maxPartitionLength": 50,
      "dp:maxInfluencedPartitions": 1,
      "dp:maxPartitionContribution": 2
    },
    {
      "@type": "dp:PartitionKey",
      "dp:components": {
        "sex": {
          "@type": "dp:PartitionKey",
          "dp:partitionValue": "FEMALE"
        },
        "flipper_length_mm": {
          "@type": "dp:PartitionKey",
          "dp:lowerBound": 150.0,
          "dp:upperBound": 200.0,
          "dp:lowerInclusive": true,
          "dp:upperInclusive": false
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
| $l_1$      | Total changed rows                    | Derived: $l_1=l_0 * l_\infty$ |

### 1.8 Visual Overview

```
csvw:Table ⊂ dp:DPBounded
 ├─ dp:DPBounds
 │
 ├─ csvw:tableSchema → csvw:TableSchema
 │    └─ csvw:Column ⊂ dp:GroupingKey ⊂ dp:DPBounded
 │         ├─ CSVW schema (datatype, required, default, etc.)
 │         ├─ dp:publicPartitions (DP-relevant)
 │         │    └─ dp:DPBounds
 │         └─ dp:DPBounds
 │
 └─ dp:ColumnGroup ⊂ dp:GroupingKey ⊂ dp:DPBounded
      ├─ dp:columns → rdf:List(csvw:Column)
      ├─ dp:publicPartitions (DP-relevant)
      │    └─ dp:components (structural)
      │         └─ dp:DPBounds
      └─ dp:DPBounds
```

> **Full View:** [README_details.md](https://github.com/dscc-admin-ch/csvw-dp/blob/main/README_details.md)

---

## 2. Constraints

CSVW-DP enforces constraints to ensure both semantic correctness and DP validity. Constraints apply at table, column, multi-column group, and partition levels.

All constraints assume the recursive `dp:PartitionKey` / `dp:components` model.

### 2.1 Table-Level Constraints

Applied to `csvw:Table`:

| Property                        | Constraint / Rule                                          |
| ------------------------------- | ---------------------------------------------------------- |
| `dp:partitionLength` (if known) | Must equal `dp:maxPartitionLength` at the table level      |
| `dp:maxPartitionLength`         | ≤ `dp:maxPartitionLength` of any enclosing or parent level |
| `dp:maxInfluencedPartitions`    | ≤ cumulative constraints from grouping keys and partitions |
| `dp:maxPartitionContribution`   | ≤ maximum allowed contribution at parent or table level    |
| `dp:maxNumPartitions`           | Table-level structural bound (if declared)                 |


### 2.2 Column-Level Constraints

Applied to `csvw:Column` used as a grouping key:

| Rule                                          | Meaning / Enforcement                                         |
| --------------------------------------------- | ------------------------------------------------------------- |
| `required = true → dp:nullableProportion = 0` | Columns marked required cannot be nullable                    |
| `dp:privacyId = true`                         | Column must **not participate in DP aggregation** (no bounds) |
| `dp:publicPartitions` values                  | Must match column datatype (`string`, `number`, etc.)         |
| `dp:lowerBound ≤ dp:upperBound` (numeric)     | Numeric partitions must have consistent bounds                |
| `dp:lowerInclusive`, `dp:upperInclusive`      | Boolean flags must be present for numeric partitions          |

Note: Optional columns may declare null fractions; this can affect `dp:maxPartitionLength` calculations.


### 2.3 Multi-Column Grouping Worst-Case Bounds

For `dp:ColumnGroup` entities:

| Property                      | Worst-case derivation / Rule                                                                          |
| ----------------------------- | ----------------------------------------------------------------------------------------------------- |
| `dp:maxPartitionLength`       | ≤ `min(dp:maxPartitionLength)` of constituent columns/partitions                                      |
| `dp:maxNumPartitions`         | ≤ product of per-column `dp:maxNumPartitions`                                                         |
| `dp:maxInfluencedPartitions`  | ≤ `min(dp:maxInfluencedPartitions)` of constituent columns                                            |
| `dp:maxPartitionContribution` | ≤ `min(dp:maxPartitionContribution)` of constituent columns                                           |
| `dp:publicPartitions`         | Must match the **subset of Cartesian product** of per-column partitions, expressed as `dp:components` |

Notes:
- Declaring dp:publicPartitions is only allowed if all columns in the group declare `dp:publicPartitions`.
- dp:components in each partition key must reference columns in dp:columns, and the referenced columns must exist in the table schema.

| Rule                                               | Meaning / Enforcement                                             |
| -------------------------------------------------- | ----------------------------------------------------------------- |
| No `dp:privacyId` column in group                  | Privacy identifiers cannot participate in grouped DP computations |
| If any column lacks `dp:publicPartitions`          | The group **must not declare** `dp:publicPartitions`              |
| If any column lacks `dp:maxNumPartitions`          | The group **must not declare** `dp:maxNumPartitions`              |
| `dp:components` keys must match `dp:columns`       | Structural consistency                                            |
| `dp:components` partition values must respect type | Each per-column `dp:PartitionKey` must obey column datatype       |
| Overrides of DP bounds in `dp:components`          | Allowed but must be ≤ group-level DP bounds                       |

Notes:
- The recursion of `dp:PartitionKey` ensures both categorical and numeric dimensions are validated consistently.
- Each `dp:PartitionKey` in `dp:components` inherits bounds from the parent unless explicitly overridden.


### 2.4 Partition-Level Constraints

Applied to `dp:PartitionKey`:

| Rule                                                                                             | Meaning / Enforcement                                                        |
| ------------------------------------------------------------------------------------------------ | ---------------------------------------------------------------------------- |
| `dp:maxNumPartitions = 1`                                                                        | Each partition is a single group; structural only                            |
| `dp:components` keys must match parent columns                                                   | Structural consistency                                                       |
| Categorical partitions must declare `dp:partitionValue`                                          | Numeric partitions must declare `dp:lowerBound`/`dp:upperBound`              |
| Numeric partitions bounds must be consistent                                                     | `lowerBound ≤ upperBound`; `lowerInclusive`/`upperInclusive` must be boolean |
| DP bounds (`dp:maxPartitionLength`, `dp:maxInfluencedPartitions`, `dp:maxPartitionContribution`) | Must be ≤ bounds declared at parent grouping key level                       |



> SHACL enforcement for all levels: [`csvw-dp-constaints.ttl`](https://github.com/dscc-admin-ch/csvw-dp/blob/main/csvw-dp-constaints.ttl)

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
