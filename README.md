# CSVW Differential Privacy Extension (CSVW-DP) Vocabulary

## Overview

Differential privacy (DP) requires metadata such as:

* Maximum number of rows contributed by a single individual
* Maximum group (partition) sizes
* Bounds on the number of partitions a person can influence
* Constraints preventing overflow or numerical instability during aggregation

These assumptions are essential for meaningful DP guarantees, but the core CSVW vocabulary cannot express them.

**CSVW-DP** extends the [CSV on the Web (CSVW)](https://www.w3.org/TR/tabular-data-model/) vocabulary with a declarative, semantic, DP-aware data modeling system, allowing you to:

* Explicitly declare contribution bounds at table, column, and multi-column levels
* Express DP constraints required by most DP libraries
* Integrate seamlessly with existing CSVW metadata

See [guidelines and notes](https://github.com/dscc-admin-ch/csvw-dp/blob/main/guidelines.md).
Example metadata: [Penguin dataset YAML](https://github.com/dscc-admin-ch/csvw-dp/blob/main/penguin_metadata.yaml).

CSVW-DP separates concerns into:

1. **Vocabulary** – What the data represents
2. **Constraints** – What values are allowed

---

## 1. Vocabulary

### Namespace & Definitions

* **Default namespace:** `https://github.com/dscc-admin-ch/csvw-dp/csvw-dp-ext#`
* **Machine-readable definitions:** `csvw-dp-ext.ttl`
* Motivation: DP libraries require additional metadata beyond CSVW core. See [dp_libraries.md](https://github.com/dscc-admin-ch/csvw-dp/blob/main/dp_libraries.md) for an overview of the main DP libraries and the parameters they use.

### 1.1 Table-Level Properties

| Term                  | Type             | Meaning                                                            |
| --------------------- | ---------------- | ------------------------------------------------------------------ |
| `dp:maxTableLength`   | positive integer | Upper bound on total rows (avoids overflow/numerical instability). |
| `dp:tableLength`      | positive integer | Actual number of rows (if known).                                  |
| `dp:maxContributions` | positive integer | Max rows contributed by a single individual.                       |

> **Reference:** [Casacuberta et al., 2022](https://dl.acm.org/doi/pdf/10.1145/3548606.3560708)

**Note:** CSVW’s `csvw:tableSchema` still defines table structure.

### 1.2 Column-Level Properties

| Term                    | Type         | Meaning                                          |
| ----------------------- | ------------ | ------------------------------------------------ |
| `dp:privacyId`          | boolean      | True if column identifies individuals/units.     |
| `dp:nullableProportion` | decimal 0–1  | Fraction of null values (approximate modeling).  |

**Notes:**

* `dp:nullableProportion` is optional; coarse bounds are sufficient.
* Existing CSVW terms are reused where applicable: `datatype`, `format`, `minimum`, `maximum`, `required`, `default`.

### 1.3 Groupable Columns

#### Abstract Class: `dp:Groupable`

Represents any entity usable for DP groupings.

| Class            | Meaning                    |
| ---------------- | -------------------------- |
| `csvw:Column`    | Single column group key    |
| `dp:ColumnGroup` | Composite key (2+ columns) |

#### Per-Column DP Bounds

| Term                          | Type             | Meaning                               |
| ----------------------------- | ---------------- | ------------------------------------- |
| `dp:maxPartitionLength`       | positive integer | Max group size                        |
| `dp:maxNumPartitions`         | positive integer | Max distinct group keys               |
| `dp:maxInfluencedPartitions`  | positive integer | Max groups a person can contribute to |
| `dp:maxPartitionContribution` | positive integer | Max contributions per partition       |
| `dp:publicPartitions`         | list(string)     | Declared set of partition keys (publicly known). |

> `dp:maxNumPartitions` ≠ `dp:publicPartitions` length; public partitions may be a subset of observed data.

### 1.4 Multi-Column Grouping: `dp:ColumnGroup`

* Represents a composite key (2+ columns)
* Reuses same DP bounds (`dp:maxPartitionLength`, etc.) as single columns

| Term                          | Meaning                               |
| ----------------------------- | ------------------------------------- |
| `dp:columns`                  | List of constituent columns           |
| `dp:maxPartitionLength`       | Max size of any group                 |
| `dp:maxNumPartitions`         | Max distinct groups                   |
| `dp:maxInfluencedPartitions`  | Max groups a person can contribute to |
| `dp:maxPartitionContribution` | Max contributions per group           |
| `dp:publicPartitions`         | Set of partition keys (publicly known). |

**Public partitions vs format:**

* `datatype`/`format` describe the value domain
* `dp:publicPartitions` describes the DP grouping universe
* Missing values (NaN) are assumed publicly known if `required=false` and `dp:publicPartitions` is present

### 1.5 Derived Columns (`dp:DerivedColumn`)

* Subclass of `csvw:Column`
* Identified with `csvw:virtual = true`
* Tracks transformations and DP bounds

| Term                         | Type         | Meaning                      |
| ---------------------------- | ------------ | ---------------------------- |
| `csvw:virtual`               | boolean      | True if derived/preprocessed |
| `dp:derivedFrom`             | list(string) | Source columns               |
| `dp:transformationType`      | string       | Preprocessing operation      |
| `dp:transformationArguments` | object       | Transformation parameters    |

> Derived columns allow tighter DP bounds than raw data.

### 1.6 Relation to DP Metrics

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
 │    └─ csvw:column → csvw:Column ⊂ dp:Groupable
 │         ├─ core CSVW schema
 │         ├─ DP group-by bounds
 │         └─ derived-column semantics
 │
 └─ dp:ColumnGroup ⊂ dp:Groupable
      └─ multi-column DP bounds
```

> **Full View:** [README_details.md](https://github.com/dscc-admin-ch/csvw-dp/blob/main/README_details.md)

---

## 2. Constraints

### 2.1 Worst-Case Bounds for Multi-Column Aggregations

| Property                      | Worst-case                           | Rule / Derivation                 | Notes                            |
| ----------------------------- | ------------------------------------ | --------------------------------- | -------------------------------- |
| `dp:publicPartitions`         | Cartesian product of columns         | If all grouped columns declare it | Otherwise group must not declare |
| `dp:maxPartitionLength`       | `min(dp:maxPartitionLength_i)`       | Conservative upper bound          | Use min if missing values exist  |
| `dp:maxNumPartitions`         | `≤ ∏ dp:maxNumPartitions_i`          | Product of per-column maxima      | Must not declare if any missing  |
| `dp:maxInfluencedPartitions`  | `min(dp:maxInfluencedPartitions_i)`  | Min of known columns              | -                                |
| `dp:maxPartitionContribution` | `min(dp:maxPartitionContribution_i)` | Min of known columns              | -                                |

> **Example:** [README_details.md](https://github.com/dscc-admin-ch/csvw-dp/blob/main/README_details.md)

### 2.2 Column-Level Constraints

| Rule                                             | Meaning                                           |
| ------------------------------------------------ | ------------------------------------------------- |
| `required=true → dp:nullableProportion=0`        | Required columns cannot be nullable               |
| `dp:privacyId=true` cannot declare DP bounds     | Privacy ID columns cannot be used for aggregation |
| `dp:publicPartitions` values must match datatype | Type safety                                       |

### 2.3 Multi-Column Group Constraints

| Rule                                                              | Meaning                          |
| ----------------------------------------------------------------- | -------------------------------- |
| No `dp:privacyId` columns in group                                | Privacy ID cannot participate    |
| If any column lacks `dp:publicPartitions`, group must not declare | Prevent unsafe assumptions       |
| If any column lacks `dp:maxNumPartitions`, group must not declare | Avoid underestimating partitions |

### 2.4 Table-Level Consistency Rules

| Property                      | Constraint              |
| ----------------------------- | ----------------------- |
| `dp:tableLength`              | = `dp:maxTableLength`   |
| `dp:maxPartitionLength`       | ≤ `dp:maxTableLength`   |
| `dp:maxInfluencedPartitions`  | ≤ `dp:maxContributions` |
| `dp:maxPartitionContribution` | ≤ `dp:maxContributions` |

> SHACL enforcement: [`metadata_constraints.ttl`](https://github.com/dscc-admin-ch/csvw-dp/blob/main/csvw-dp-ext-constaints.ttl)

### 2.5 Derived Columns & Transformations

* Derived columns tighten DP bounds via transformations: `filter`, `bin`, `clip`, `truncate`, `recode`, `concatenation`, `onehot`
* Virtual columns declared with `csvw:virtual=true` and `dp:derivedFrom`
* Transformation arguments define DP effects (max partitions, contributions, etc.)

| Transformation       | DP Effect                                   |
| -------------------- | ------------------------------------------- |
| clipping             | ↓ sensitivity (tightens min/max)            |
| truncation           | ↓ per-person contributions                  |
| fill_na_constant     | neutral                                     |
| fill_na_data_derived | ↑ sensitivity                               |
| filter               | ↓ table length, ↓ contributions             |
| binning              | ↓ partitions, defines `dp:publicPartitions` |
| recoding             | ↓ domain size                               |
| concatenation        | ↑ domain size                               |
| onehot               | ↑ dimensionality                            |

- truncation/clipping/fill_na_constant → safest
- concatenation/onehot/fill_na_data_derived → riskiest.

---

## 3. Summary of Files

| File                          | Purpose                             |
| ----------------------------- | ----------------------------------- |
| `README.md`                   | Description, Motivation             |
| `csvw-dp-ext.ttl`             | Vocabulary definition (OWL + RDFS)  |
| `csvw-dp-ext-context.jsonld`  | JSON-LD context                     |
| `csvw-dp-ext-constraints.ttl` | SHACL validation rules              |
| `penguin_metadata.json`       | Example metadata                    |
| `dp_libraries.md`             | Mapping to DP libraries             |
| `validate_metadata.py`        | Metadata validator                  |
| `make_metadata_from_data.py`  | Infer baseline CSVW metadata        |
| `make_dummy_from_metadata.py` | Dummy data generator                |

`make_metadata_from_data.py` and `make_dummy_from_metadata.py` still need DerivedColumn, GroupColumn logic (maybe).
