# CSVW Safe Modeling Extension (CSVW-SAFE) Vocabulary

THIS IS WORK IN PROGRESS!!!!!!!

## Overview

Many datasets cannot be directly shared due to privacy, legal, or governance constraints.
However, it is often possible, and highly valuable, to share safe, public assumptions about their structure.

These assumptions may include:

* Structural information (schema, keys, allowed partitions)
* Statistical characteristics (null proportions, value domains, approximate cardinalities)
* Logical constraints between columns
* Bounds on how individuals may contribute to the dataset

Such metadata enables:

* Automatic computation of worst-case sensitivity for Differential Privacy (DP)
* Generation of structurally valid dummy datasets
* Safe data discovery without direct access to the underlying data
* Interoperating with existing CSVW tooling and DP libraries

The core [CSV on the Web (CSVW)](https://www.w3.org/TR/tabular-data-model/) vocabulary describes tabular structure but cannot express these additional safe modeling assumptions.

**CSVW-SAFE** extends CSVW with a declarative, machine-readable vocabulary for describing public, non-sensitive constraints and assumptions about tabular datasets (not measured properties).

It does not guarantee privacy by itself. 
It enables automated computation of query sensitivity for differential privacy mechanisms.

See:

* [Guidelines and notes](https://github.com/dscc-admin-ch/csvw-safe/blob/main/documentation/guidelines.md)
* [DP libraries overview](https://github.com/dscc-admin-ch/csvw-safe/blob/main/documentation/dp_libraries.md)
* Example metadata: [Penguin dataset.json](https://github.com/dscc-admin-ch/csvw-safe/blob/main/manual_penguin_metadata.json) of the sklearn penguin dataset

---


## 1. Overview

* **Default namespace:** `https://w3id.org/csvw-safe#`
* **Vocabulary definitions:** `csvw-safe-vocab.ttl`
* **JSON-LD context:** `csvw-safe-context.jsonld`


CSVW-SAFE models three independent aspects of a dataset:
| Aspect                 | Question answered                                |
| ---------------------- | ------------------------------------------------ |
| Structure              | What values and partitions are valid?            |
| Contribution           | How much can one individual affect results?      |


In CSVW-SAFE, there are 4 main objects on which the properties apply:
| Class                   | Purpose                        |
| ----------------------- | ------------------------------ |
| `csvw:Table`            | Dataset-level guarantees       |
| `csvw:Column`           | Column schema and grouping key |
| `csvw-safe:ColumnGroup` | Multi-column grouping key      |
| `csvw-safe:Partition`   | A possible group of rows       |

- `csvw:Table`  are tables as described in `csvw`. A `csvw:Table` contains a `csvw:TableSchema` (with a list of `csvw:Columns`) and optionnaly a `csvw-safe:AdditionalInformation` (with a list of `csvw-safe:ColumnGroup` and their partitions).
- `csvw:Column` are columns as described in `csvw`.
- `csvw-safe:ColumnGroup` represents a group of columns. It is useful to describe contributions and partitions after a groupby on a group of columns.
- `csvw:Column` and `csvw-safe:ColumnGroup` can have partitions. If `csvw-safe:publicPartitions` is declared, it contains a list of `csvw-safe:Partition`.
- A `csvw-safe:Partition` represents one possible group of rows. For details on `csvw-safe:Partition`, see point 2.4 below.
CSVW-SAFE structural and contribution properties apply on these four main classes. 

![Overview](images/csvw-safe_structure.png)


## 2. Differential Privacy Extensions

A privacy unit defines dataset adjacency. Two datasets are neighbours if and only if all rows associated with one value of the privacy unit are added or removed.

CSVW-SAFE assumes bounded user-level differential privacy where neighboring datasets differ by all rows associated with one or more privacy units. 
When multiple privacy units exist, guarantees apply independently per privacy unit and mechanisms must be parameterized by the chosen privacy unit. DP guarantees apply per declared unit independently.

Partitions referenced in contribution bounds are defined relative to a grouping space, not the physical table.

A privacy unit is an identifier representing an individual or entity whose data must be protected (e.g. `patient_id`, `user_id`, `hospital_id`).

Contribution bounds describe how much influence one privacy unit can have on the output.

### 2.1 All Levels

We define 6 new terms that can be used to infer DP bounds. 

| Name                         | Table    | Partition | Column         | ColumnGroup    |
|------------------------------|---------:|----------:|---------------:|----------------|
| `csvw-safe:maxContributions` | Yes (C)  | Yes       | No             | No             |
| `csvw-safe:maxInfluencedPartitions`| 1  | 1         | Yes            | Yes            |
| `csvw-safe:maxLength`        | Yes (C)  | Yes       | No             | No             |
| `csvw-safe:publicLength`     | Yes      | Yes       | No             | No             |
| `csvw-safe:maxNumPartitions` | No       | No        | Yes            | Yes            |
| `csvw-safe:publicPartitions` | No       | No        | Yes            | Yes            |

(C): means compulsory to apply DP. The rest is optional and will avoid wasting budget on public information and avoir overstimating sensitivity.

`csvw-safe:maxContributions` ($l_\infty$) is the maximum number of rows belonging to the same privacy unit within a single partition.
- At the table level, it is the maximum number of rows a privacy unit may contribute to the entire dataset. This bound governs sensitivity of queries without grouping. It is compulsory to apply DP.
- At the partition level, it is the maximum number of rows in the partition which concern the privacy unit.

`csvw-safe:maxInfluencedPartitions` ($l_0$) is the maximum number of partitions in which the same privacy unit may appear. It is evaluated relative to the grouping key (Column or ColumnGroup) used in the query.
- At the table level, it does not make sense and is 1.
- At the partition level, it does not make sense and is 1.
- At the column level, it is the number of partitions of the column (after a groupy) that can be affected by an individual.
- At the multiple column level, it is the number of partitions of the group of columns (after a groupby) that can be affected by an individual. In the worst case, the product of the number of partitions of all individual columns.

**Note**:These parameters allow systems to determine the maximum number of rows that may change if one privacy unit is added or removed. 
The total number of rows a privacy unit may influence $l_1 = l_0 \cdot l_\infty$ is not defined as a new word as it depends on the query and $l_\infty$ and $l_0$.

`csvw-safe:maxLength` is the maximum theoretical number of rows. Is also enables to compute additional noise requirements in case of overflow when doing some operations. See reference: [Casacuberta et al., 2022](https://dl.acm.org/doi/pdf/10.1145/3548606.3560708).
- At the table level, it is the maximum theoretical number of rows in the table. It is compulsory to apply DP.
- At the partition level, it is the maximum theoretical number of rows in the partition.

`csvw-safe:publicLength` is the exact number of rows if it is known (if it is public information). This is only safe if the length is invariant across neighbouring datasets (invariant under the adjacency relation). If given, it can save budget for some operations. For instance, for a mean, all the budget is spent for the sum and then divided by publicLength instead of splitting the budget in half for a count and a sum.
- At the table level, it is the number of rows in the table. 
- At the partition level, it is the number of rows in the partition.
- It does not make sense at `csvw-safe:Column` and `csvw-safe:ColumnGroup` level as it is the same as at `csvw:Table` level.

`csvw-safe:maxNumPartitions` is the maximum number of partitions after a groupby operations on a `csvw-safe:Column` or a `csvw-safe:ColumnGroup`.
- At the column level, it is the number of different categories in the column.
- At the group of columns level, it is the number of different partitions that can be produced by grouping multiple columns (cartesian product of the partitions of each column in the simplest case).
If `csvw-safe:maxInfluencedPartitions` is not public, then `csvw-safe:maxNumPartitions` can be used as an upper bound, if known and useful to reduce sensitivity.

`csvw-safe:publicPartitions` is the list of known public partitions in a column or group of column. They are made of `csvw-safe:Partition` (see section 2.3 on partitions level structural properties).
- At the column level, it is the list of public `csvw-safe:Partition` of a given column.
- At the group of columns level, it is the list of public `csvw-safe:Partition` produced by grouping multiple columns.
This enables to avoid spending budget (delta) to release partitions name if already public.

Along with `csvw-safe:publicPartitions`, the term `csvw-safe:exhaustivePartitions` is used. If all partitions are public and given in `csvw-safe:publicPartitions`, then it is True, otherwise, it is False. `csvw-safe:exhaustivePartitions` applies to `csvw-safe:Column` and `csvw-safe:ColumnGroup` objects. Null values form an implicit partition unless prohibited by `required=true`.


### 2.2 Contribution with respect to an privacy unit

`csvw-safe:maxContributions` ($l_\infty$) and `csvw-safe:maxInfluencedPartitions` ($l_0$) are defined wrt to a privacy unit.

A `csvw-safe:ContributionKey` defines contribution bounds for a specific privacy unit and determines the adjacency relation used for differential privacy guarantees.

If there is only one privacy unit, then it can be defined at the top level *"csvw-safe:privacyUnit": "patient_id"* and then,
At column level `csvw-safe:maxInfluencedPartitions`:
```
{
  "@type": "csvw:Column",
  "name": "disease",
  "csvw-safe:maxInfluencedPartitions": 10
}
```
and at partition level `csvw-safe:maxContributions`:
```
"csvw-safe:publicPartitions":[
   {
      "@type":"csvw-safe:Partition",
      "csvw-safe:partitionValue":"Adelie",
      "csvw-safe:maxContributions": 1
   },
   {
      "@type":"csvw-safe:Partition",
      "csvw-safe:partitionValue":"Chinstrap",
      "csvw-safe:maxContributions": 1
   }
]
```


However, if there are two privacy units *patient_id* and *hospital_id*, then they must be defined for each individually.

At column level `csvw-safe:maxInfluencedPartitions`:
```
{
  "@type": "csvw:Column",
  "name": "disease",
  "csvw-safe:PrivacyContributions": [
    {
      "@type": "csvw-safe:ContributionKey",
      "csvw-safe:privacyUnit": "patient_id",
      "csvw-safe:maxInfluencedPartitions": 10,
    },
    {
      "@type": "csvw-safe:ContributionKey",
      "csvw-safe:privacyUnit": "hospital_id",
      "csvw-safe:maxInfluencedPartitions": 2,
    }
  ]
}
```
and at partition level `csvw-safe:maxContributions`:
```
"csvw-safe:publicPartitions":[
   {
      "@type":"csvw-safe:Partition",
      "csvw-safe:partitionValue":"Adelie",
      "csvw-safe:PrivacyContributions": [
        {
          "@type": "csvw-safe:ContributionKey",
          "csvw-safe:privacyUnit": "patient_id",
          "csvw-safe:maxContributions": 1,
        },
        {
          "@type": "csvw-safe:ContributionKey",
          "csvw-safe:privacyUnit": "hospital_id",
          "csvw-safe:maxContributions": 2,
        }
      ]
   },
   {
      "@type":"csvw-safe:Partition",
      "csvw-safe:partitionValue":"Chinstrap",
      "csvw-safe:PrivacyContributions": [
        {
          "@type": "csvw-safe:ContributionKey",
          "csvw-safe:privacyUnit": "patient_id",
          "csvw-safe:maxContributions": 1,
        },
        {
          "@type": "csvw-safe:ContributionKey",
          "csvw-safe:privacyUnit": "hospital_id",
          "csvw-safe:maxContributions": 1,
        }
      ]
   }
]
```

### 2.3 Minimum Metadata for Worst-Case Sensitivity

Some fields are compulsory:
- Table-level (for all privacy unit)
    - `csvw-safe:maxLength` (see [Casacuberta et al., 2022](https://dl.acm.org/doi/pdf/10.1145/3548606.3560708))
    - `csvw-safe:maxContributions` (for sensitivity)
- Continuous columns when column participates in numeric aggregation:
    - `minimum` (for sensitivity)
    - `maximum` (for sensitivity)

Declaring bounds at grouping or partition level is optional but recommended.
Providing tighter bounds reduces worst-case sensitivity and thus allows lower noise addition under a fixed privacy budget.



## 3. Structural Modeling Extensions

Structural metadata supports:

- Dummy dataset generation (generate a dataset that has the same schema and structure as the original dataset) for functionnel programming for instance.
- Public schema discovery (can already answer some questions without requiring private data access).

All standard CSVW column properties (`datatype`, `format`, `minimum`, `maximum`, `required`, `default`) are re-used as is.
In particular, for continuous columns, `minimum` and `maximum` are compusory to apply DP.

### 3.1 Column-Level Structural Properties

For structural purposes, other fields exist on the `csvw:Column`:

| Term                           | Type                                  | Meaning                                             |
| ------------------------------ | ------------------------------------- | --------------------------------------------------- |
| `csvw-safe:privacyId`          | boolean                               | True if column identifies individuals/units         |
| `csvw-safe:nullableProportion` | decimal (0–1)                         | Approximate fraction of null values                 |
| `csvw-safe:dependsOn`          | column reference                      | Declares dependency on another column               |
| `csvw-safe:how`                | enum (`bigger`, `smaller`, `mapping`) | Type of dependency                                  |
| `csvw-safe:mapping`            | object                                | Required if `how = mapping`                         |

**Dependency Rules**
- `dependsOn` and `how` MUST be provided together.
- If `how = mapping`, then `mapping` MUST be provided.

**Notes**
- nullableProportion improves modeling beyond csvw:required.
- maxNumPartitions describes grouping universe size but does not affect sensitivity unless combined with DP bounds.
- multiple columns may have `csvw-safe:privacyId=true`. In these cases, DP contributions (section 3) must be provided per privacy unit.

### 3.2 ColumnGroup-Level Structural Properties
`csvw-safe:ColumnGroup` represents a grouping key composed of multiple columns

| Property            | Meaning                             |
| ------------------- | ----------------------------------- |
| `csvw-safe:columns` | Ordered list of constituent columns |

If a `csvw-safe:ColumnGroup` is declared, all referenced columns must exist in the table schema.

A `ColumnGroup` defines a joint grouping space. It does not automatically enumerate all combinations; explicit partitions may optionally restrict this space (see Partitions-Level below).

### 3.3 Partition-Level Structural Properties

A `csvw-safe:publicPartitions` is a list of `csvw-safe:Partition` based on
- A categorical value
- A numeric interval
- A composite multi-column constraint

A partition is a publicly defined subset of rows determined solely by public attributes.
A row belongs to a partition iff it satisfies its `PartitionKey` constraints. A `csvw-safe:Partition` is the conjunction of one or more PartitionKey objects.


Partitions are used to define:
- histogram buckets
- grouping outputs
- contribution bounds

Partition-level bounds MUST be <= bounds of their parent scope.

If all `csvw-safe:Partition` are given in `csvw-safe:publicPartitions` then `csvw-safe:exhaustivePartitions` is True, otherwise, it is False.

A `csvw-safe:PartitionKey` is defined by:

| Property                   | Type                           | Meaning                                           |
| -------------------------- | ------------------------------ | ------------------------------------------------- |
| `csvw-safe:partitionValue` | literal                        | Categorical partition value                       |
| `csvw-safe:lowerBound`     | decimal                        | Lower bound (numeric partition)                   |
| `csvw-safe:upperBound`     | decimal                        | Upper bound (numeric partition)                   |
| `csvw-safe:lowerInclusive` | boolean                        | Whether lower bound is inclusive (default: true)  |
| `csvw-safe:upperInclusive` | boolean                        | Whether upper bound is inclusive (default: false) |
| `csvw-safe:components`     | map → `csvw-safe:PartitionKey` | Identifier of partition for multiple columns      |

For `csvw:Column` with categorical data, the partition can be identified by `csvw-safe:partitionValue`.
```
{
  "name": "sex",
  "datatype": "string",
  "csvw-safe:publicPartitions": [
    {
      "@type": "csvw-safe:PartitionKey",
      "csvw-safe:partitionValue": "MALE"
    },
    {
      "@type": "csvw-safe:PartitionKey",
      "csvw-safe:partitionValue": "FEMALE"
    }
  ]
}
```
For `csvw:Column` with continous data, the partition can be identified by `csvw-safe:lowerBound`, `csvw-safe:upperBound`, `csvw-safe:lowerInclusive` and `csvw-safe:upperInclusive` fields.
```
{
  "name": "flipper_length_mm",
  "datatype": "double",
  "minimum": 150.0,
  "maximum": 250.0,
  "csvw-safe:publicPartitions": [
    {
      "@type": "csvw-safe:PartitionKey",
      "csvw-safe:lowerBound": 150.0,
      "csvw-safe:upperBound": 200.0
    },
    {
      "@type": "csvw-safe:PartitionKey",
      "csvw-safe:lowerBound": 200.0,
      "csvw-safe:upperBound": 250.0
    }
  ]
}
```
For `csvw:ColumnGroup` with categorical data, the partition can be identified by `csvw-safe:components` and then a partition per column.
```
{
  "@type": "csvw-safe:ColumnGroup",
  "csvw-safe:columns": ["sex", "island"],
  "csvw-safe:publicPartitions": [
    {
      "@type": "csvw-safe:Partition",
      "csvw-safe:components": {
        "sex": {
          "@type": "csvw-safe:PartitionKey",
          "csvw-safe:partitionValue": "MALE"
        },
        "island": {
          "@type": "csvw-safe:PartitionKey",
          "csvw-safe:partitionValue": "Torgersen"
        }
      }
    }
  ]
}
```
Similarly for a `csvw:ColumnGroup` with categorical and continuous data, the partition can be identified by `csvw-safe:components` and then a partition per column.
```
{
  "@type": "csvw-safe:ColumnGroup",
  "csvw-safe:columns": ["sex", "flipper_length_mm"],
  "csvw-safe:publicPartitions": [
    {
      "@type": "csvw-safe:Partition",
      "csvw-safe:components": {
        "sex": {
          "@type": "csvw-safe:PartitionKey",
          "csvw-safe:partitionValue": "MALE"
        },
        "flipper_length_mm": {
          "@type": "csvw-safe:PartitionKey",
          "csvw-safe:lowerBound": 150.0,
          "csvw-safe:upperBound": 200.0,
        }
      }
    }
  ]
}
```


---


## 4. Constraints

CSVW-SAFE enforces constraints to ensure both semantic correctness and DP validity. Constraints apply at table, column, multi-column group, and partition levels.

All constraints assume the recursive `csvw-safe:PartitionKey` / `csvw-safe:components` model.

### 4.1 Table-Level Constraints

Applied to `csvw:Table`:

| Property                                   | Constraint / Rule                                                 |
| ------------------------------------------ | ----------------------------------------------------------------- |
| `csvw-safe:publicLength` (if declared)     | Must be ≤ `csvw-safe:maxLength`                                   |
| `csvw-safe:maxLength`                      | Defines the global upper bound for the dataset (single partition) |
| `csvw-safe:maxContributions`                | ≤ `csvw-safe:maxLength`                                           |
| `csvw-safe:maxNumPartitions` (if declared) | Structural upper bound on grouping universe                       |


### 4.2 Column-Level Constraints

Applied to `csvw:Column` used as a grouping key:

| Rule                                                    | Meaning / Enforcement                                                    |
| ------------------------------------------------------- | ------------------------------------------------------------------------ |
| `csvw-safe:publicPartitions` values                     | Must match column datatype (`string`, `number`, etc.)                    |
| `csvw-safe:lowerBound ≤ csvw-safe:upperBound` (numeric) | Numeric partitions must have consistent bounds                           |
| `csvw-safe:lowerInclusive`, `csvw-safe:upperInclusive`  | Must be boolean if numeric bounds are declared                           |

Note: Optional columns may declare null fractions; this can affect `csvw-safe:maxLength` calculations.


### 4.3 Multi-Column Grouping Worst-Case Bounds

For `csvw-safe:ColumnGroup` entities:

| Property                            | Worst-case derivation / Rule                                                                                        |
| ----------------------------------- | ------------------------------------------------------------------------------------------------------------------- |
| `csvw-safe:maxLength`               | ≤ `min(csvw-safe:maxLength)` of parent grouping scopes containing identical `privacyUnit`                           |
| `csvw-safe:maxNumPartitions`        | ≤ product of per-column `csvw-safe:maxNumPartitions`                                                                |
| `csvw-safe:maxInfluencedPartitions` | ≤ `min(csvw-safe:maxInfluencedPartitions)` of parent grouping scopes containing identical `privacyUnit`             |
| `csvw-safe:maxContributions`        | ≤ `min(csvw-safe:maxContributions)` of parent grouping scopes containing identical `privacyUnit`                    |
| `csvw-safe:publicPartitions`        | Must represent a subset of the Cartesian product of per-column partitions, expressed via `csvw-safe:components`     |


Notes:
- Declaring csvw-safe:publicPartitions is only allowed if all columns in the group declare `csvw-safe:publicPartitions`.
- csvw-safe:components in each partition key must reference columns in csvw-safe:columns, and the referenced columns must exist in the table schema.

Additional Group-Level Rules
| Rule                                                                    | Meaning / Enforcement                                             |
| ----------------------------------------------------------------------- | ----------------------------------------------------------------- |
| If any column lacks `csvw-safe:publicPartitions`                        | The group **must not declare** `csvw-safe:publicPartitions`       |
| If any column lacks `csvw-safe:maxNumPartitions`                        | The group **must not declare** `csvw-safe:maxNumPartitions`       |
| `csvw-safe:components` keys must match `csvw-safe:columns`              | Structural consistency                                            |
| Partition values in `csvw-safe:components` must respect column datatype | Type correctness                                                  |
| Overrides of DP bounds in `csvw-safe:PartitionKey`                      | Allowed but must be ≤ group-level DP bounds                       |


Notes:
- The recursion of `csvw-safe:PartitionKey` ensures both categorical and numeric dimensions are validated consistently.
- Each `csvw-safe:PartitionKey` in `csvw-safe:components` inherits bounds from the parent unless explicitly overridden.


### 4.4 Partition-Level Constraints

Applied to `csvw-safe:PartitionKey`:

| Rule                                                                                                | Meaning / Enforcement                                  |
| --------------------------------------------------------------------------------------------------- | ------------------------------------------------------ |
| Structural partition represents a single group                                                      | Implicit `csvw-safe:maxNumPartitions = 1`              |
| `csvw-safe:components` keys must match parent grouping columns                                      | Structural consistency                                 |
| Categorical partitions must declare `csvw-safe:partitionValue`                                      | Required for categorical columns                       |
| Numeric partitions must declare `csvw-safe:lowerBound` and `csvw-safe:upperBound`                   | Required for numeric columns                           |
| Numeric bounds must satisfy `lowerBound ≤ upperBound`                                               | Interval validity                                      |
| DP bounds (`csvw-safe:maxLength`, `csvw-safe:maxInfluencedPartitions`, `csvw-safe:maxContributions`) | Must be ≤ bounds declared at parent grouping key level |
| `csvw-safe:publicLength` (if declared)                                                              | Must be ≤ `csvw-safe:maxLength`                        |


> SHACL enforcement for all levels: [`csvw-safe-constaints.ttl`](https://github.com/dscc-admin-ch/csvw-safe/blob/main/csvw-safe-constaints.ttl)

---
## 5. Utility Files

This library provides Python utilities for generating, validating, and testing CSVW-SAFE metadata and associated dummy datasets for differential privacy (DP) development and safe data modeling workflows.

It includes four main scripts:

1. make_metadata_from_data.py
2. make_dummy_from_metadata.py
3. validate_metadata.py
4. assert_same_structure.py

This is available in a pip library `csvw-safe-lib` described in [the README.md of `csvw-safe-lib`](https://github.com/dscc-admin-ch/csvw-safe/blob/main/csvw-safe-library/README.md).

![Overview](images/utils_scripts.png)

## 6. CSVW-SAFE Framework

| File                          | Purpose                             |
| ----------------------------- | ----------------------------------- |
| `README.md`                   | Description, Motivation             |
| `csvw-safe-vocab.ttl`         | Vocabulary definition (OWL + RDFS)  |
| `csvw-safe-context.jsonld`    | JSON-LD context                     |
| `csvw-safe-constraints.ttl`   | SHACL validation rules              |
| `penguin_metadata.json`       | Example metadata                    |
| `dp_libraries.md`             | Mapping to DP libraries             |
| `validate_metadata.py`        | Metadata validator                  |
| `make_metadata_from_data.py`  | Infer baseline CSVW metadata        |
| `make_dummy_from_metadata.py` | Dummy data generator                |
| `assert_same_structure.py`    | Verify functional programming valid on dummy will be valid on real data |

---

