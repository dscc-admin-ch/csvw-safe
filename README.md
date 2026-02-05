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

### 1.1 Core Classes

**`dp:DPBounded`**

An abstract class representing any entity to which DP contribution or size bounds may be attached.

Examples:
- A grouping key
- A specific partition

DP bounds such as `dp:maxPartitionLength` or `dp:maxPartitionContribution` are defined on this class.


**`dp:GroupingKey`**

A subclass of `dp:DPBounded`.

Represents an entity that defines a group-by key space for DP aggregation.

Instances include:
- A single `csvw:Column`
- A composite grouping (`dp:ColumnGroup`)

Grouping keys may declare:
- How many partitions exist
- What the public partitions are
- How users may contribute across partitions


**`dp:PartitionKey`**

A subclass of `dp:DPBounded`.
Represents one publicly declared partition within a grouping key.

A partition key may describe:
- A categorical value (e.g. "male")
- A numeric interval (e.g. [0, 10))
- A combination of values for multi-column groupings

Partition keys may declare exact bindings of grouping columns to values via dp:partitionBindings, e.g.:
```
dp:partitionBindings:
  species: Adelie
  island: Torgersen
```

This allows non-Cartesian, explicitly enumerated partitions while remaining declarative.
Partition keys may optionally override DP bounds locally.


**`dp:ColumnGroup`**

A subclass of `dp:GroupingKey`.
Represents a grouping key formed by two or more columns.


### 1.2 Table-Level Properties

Applied to `csvw:Table`

| Term                  | Type             | Meaning                                                            |
| --------------------- | ---------------- | ------------------------------------------------------------------ |
| `dp:maxTableLength`   | positive integer | Upper bound on total rows (avoids overflow/numerical instability). |
| `dp:tableLength`      | positive integer | Actual number of rows (if known).                                  |
| `dp:maxContributions` | positive integer | Max rows contributed by a single individual.                       |

> **Reference:** [Casacuberta et al., 2022](https://dl.acm.org/doi/pdf/10.1145/3548606.3560708)

CSVW's `csvw:tableSchema` still defines table structure.

### 1.3 Column-Level Properties

Applied to `csvw:Column`

| Term                    | Type         | Meaning                                          |
| ----------------------- | ------------ | ------------------------------------------------ |
| `dp:privacyId`          | boolean      | True if column identifies individuals/units.     |
| `dp:nullableProportion` | decimal 0–1  | Fraction of null values (approximate modeling).  |

Standard CSVW terms properties (`datatype`, `format`, `minimum`, `maximum`, `required`, `default`) are re-used as is.
`dp:nullableProportion` is optional and mostly for modeling. (TODO: see how to handle).

### 1.4 Grouping Keys and Public Partitions

Grouping keys (single columns or column groups) may declare public partitions.

`dp:publicPartitions`
- Domain: `dp:GroupingKey`
- Type: list

Each entry may be:
- A literal value (simple categorical partition), or
- A `dp:PartitionKey` object (structured partition)

Partition keys may include `dp:partitionBindings` mapping column names to values.
This is especially useful for multi-column groups where the partition is a subset of the Cartesian product.

Public partitions define the DP grouping universe and may be a strict subset of observed values.

| Concept                    | Meaning                                             |
| -------------------------- | --------------------------------------------------- |
| **`dp:GroupingKey`**       | Something you can group by (column or column group) |
| **`dp:PartitionKey`**      | One allowed group value for a grouping key          |
| **`dp:partitionBindings`** | Mapping from grouping columns → values (optional)   |
| **`dp:publicPartitions`**  | The explicit universe of allowed groups             |


### 1.5 Partition Key Structure

Applied to `dp:PartitionKey`

| Property            | Type    | Meaning                            |
| ------------------- | ------- | ---------------------------------- |
| `dp:partitionValue` | literal | Categorical partition value        |
| `dp:lowerBound`     | decimal | Lower bound (inclusive by default) |
| `dp:upperBound`     | decimal | Upper bound (exclusive by default) |
| `dp:partitionLabel` | string  | Human-readable label               |
| `dp:partitionBinding` |   |             |

If bounds are omitted, the partition is treated as categorical.
DP bounds on `dp:PartitionKey` refine or override the bounds declared on the enclosing `dp:GroupingKey`.


### 1.6 Differential Privacy Bounds

Applied to `dp:DPBounded`

| Term                          | Meaning                           | DP interpretation    |
| ----------------------------- | --------------------------------- | -------------------- |
| `dp:maxPartitionLength`       | Max rows in scope                 | Partition size bound |
| `dp:maxPartitionContribution` | Max rows per person per partition | $l_\infty$            |
| `dp:maxInfluencedPartitions`  | Max partitions per person         | $l_0$                |

Applied to `dp:GroupingKey` only:

| Term                  | Meaning                               |
| --------------------- | ------------------------------------- |
| `dp:maxNumPartitions` | Maximum number of distinct partitions |

`dp:maxNumPartitions` $\neq$ `dp:publicPartitions` length; public partitions may be a subset of observed data.

### 1.7 Multi-Column Grouping: `dp:ColumnGroup`

| Property               | Meaning                                                           |
| ---------------------- | ----------------------------------------------------------------- |
| `dp:columns`           | Constituent columns                                               |
| `dp:publicPartitions`  | Optional public partitions                                        |
| `dp:partitionBindings` | Optional mapping for each partition (subset of Cartesian product) |
| `dp:maxNumPartitions`  | Max distinct groups                                               |
| Other DP bounds        | Same as single-column grouping                                    |


### 1.8 Relation to DP Metrics

| DP Metric  | Meaning                               | CSVW-DP Term                  |
| ---------- | ------------------------------------- | ----------------------------- |
| $l_0$      | Affected partitions per person        | `dp:maxInfluencedPartitions`  |
| $l_\infty$ | Max contribution within one partition | `dp:maxPartitionContribution` |
| $l_1$      | Total changed rows                    | Derived: `l₀ × l∞`            |

### 1.7 Visual Overview

```
csvw:Table
 ├─ dp:maxTableLength
 ├─ dp:tableLength
 ├─ dp:maxContributions
 │
 ├─ csvw:tableSchema → csvw:TableSchema
 │    └─ csvw:Column ⊂ dp:GroupingKey ⊂ dp:DPBounded
 │         ├─ CSVW schema
 │         ├─ dp:publicPartitions
 │         │    └─ optional dp:partitionBindings
 │         └─ DP bounds
 │
 └─ dp:ColumnGroup ⊂ dp:GroupingKey ⊂ dp:DPBounded
      ├─ dp:columns
      ├─ dp:publicPartitions
      │    └─ optional dp:partitionBindings
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
