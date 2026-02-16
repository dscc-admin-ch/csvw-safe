# CSVW-SAFE Constraints

CSVW-SAFE enforces constraints to ensure:

- Structural consistency of the admissible dataset universe
- Sound worst-case sensitivity computation
- Compatibility with declared privacy model

## 4.1 Table-Level Constraints

Applied to `csvw:Table`:

### 4.1.1 Mandatory for DP-calibratable metadata

The following properties are required if the dataset is intended for DP calibration:
| Property                            | Constraint                                |
| ----------------------------------- | ----------------------------------------- |
| `csvw-safe:public.privacyUnit`      | MUST reference an existing column         |
| `csvw-safe:bounds.maxLength`        | MUST be declared                          |
| `csvw-safe:bounds.maxContributions` | MUST be declared and ≤ `bounds.maxLength` |

### 4.1.2 Optional but validated

| Property                            | Constraint                                                               |
| ----------------------------------- | ------------------------------------------------------------------------ |
| `csvw-safe:public.length`           | If declared, MUST be ≤ `bounds.maxLength` and invariant across neighbors |
| `csvw-safe:bounds.maxNumPartitions` | If declared at table level, MUST be ≥ 1                                  |

### 4.1.3 Global consistency rules

- `bounds.maxContributions` (table level) represents the maximum rows one privacy unit may contribute to the entire dataset.
- `bounds.maxLength` represents the maximum total number of rows in the dataset universe.
- Table-level bounds apply as upper bounds to all child grouping scopes unless explicitly restricted further.

## 4.2 Column-Level Constraints

Applied to `csvw:Column`.
Columns act as implicit single-column GroupingKey.

### 4.2.1 Structural constraints

| Rule                                 | Enforcement                                                             |
| ------------------------------------ | ----------------------------------------------------------------------- |
| Numeric column used in aggregation   | MUST declare `minimum` and `maximum`                                    |
| `public.partitions` (if declared)    | MUST match column datatype                                              |
| Categorical partitions               | MUST use `partitionValue`                                               |
| Numeric partitions                   | MUST declare `lowerBound` AND `upperBound`                              |
| Numeric bounds                       | MUST satisfy `lowerBound ≤ upperBound`                                  |
| `public.exhaustivePartitions = true` | Number of partitions MUST equal `bounds.maxNumPartitions` (if declared) |

### 4.2.2 DP bounds constraint

| Property                                  | Constraint                                                     |
| ----------------------------------------- | -------------------------------------------------------------- |
| `bounds.maxGroupsPerUnit`                 | ≤ `bounds.maxNumPartitions` (if declared)                      |
| Partition-level `bounds.maxContributions` | MUST be ≤ column-level `bounds.maxContributions` (if declared) |
| Partition-level `bounds.maxLength`        | MUST be ≤ column-level `bounds.maxLength` (if declared)        |

If column-level bounds are omitted, table-level bounds apply.

## 4.3 Multi-Column GroupingKey Constraints

Applied to `csvw-safe:GroupingKey`.

### 4.3.1 Structural consistency

| Rule                | Enforcement                                |
| ------------------- | ------------------------------------------ |
| `columns`           | MUST reference existing columns            |
| `columns`           | MUST contain at least two distinct columns |
| `public.partitions` | MUST use `components`                      |
| `components` keys   | MUST exactly match declared `columns`      |
| Component predicate | MUST satisfy datatype rules of its column  |

### 4.3.2 Partition universe constraints
| Rule                                          | Enforcement                                                         |
| --------------------------------------------- | ------------------------------------------------------------------- |
| `public.partitions`                           | MUST represent subset of Cartesian product of per-column partitions |
| If any column lacks `public.partitions`       | Group MUST NOT declare `public.partitions`                          |
| If any column lacks `bounds.maxNumPartitions` | Group MUST NOT declare `bounds.maxNumPartitions`                    |
| `bounds.maxNumPartitions`                     | MUST be ≤ product of per-column `bounds.maxNumPartitions`           |

### 4.3.3 DP bounds consistency

For a grouping key G composed of columns $C_1, C_2, ..., C_n$:
| Property                  | Constraint                                        |
| ------------------------- | ------------------------------------------------- |
| `bounds.maxLength`        | ≤ table-level `bounds.maxLength`                  |
| `bounds.maxContributions` | ≤ table-level `bounds.maxContributions`           |
| `bounds.maxGroupsPerUnit` | ≤ product of per-column `bounds.maxGroupsPerUnit` |
| Partition-level overrides | MUST be ≤ grouping-level bounds                   |



## 4.4 Partition-Level Constraints

Applied to `csvw-safe:Partition`.

A Partition represents exactly one output coordinate.

### 4.4.1 Structural constraints
| Rule                         | Enforcement                                |
| ---------------------------- | ------------------------------------------ |
| MUST declare `predicate`     | Required                                   |
| Categorical partition        | MUST declare `partitionValue`              |
| Numeric partition            | MUST declare `lowerBound` and `upperBound` |
| Multi-column partition       | MUST use `components`                      |
| `components` keys            | MUST match parent grouping columns         |
| Numeric intervals            | MUST satisfy `lowerBound ≤ upperBound`     |
| Intervals SHOULD NOT overlap | unless explicitly allowed                  |


### 4.4.2 DP bounds constraints

| Property                      | Constraint                   |
| ----------------------------- | ---------------------------- |
| `bounds.maxContributions`     | ≤ parent grouping bound      |
| `bounds.maxLength`            | ≤ parent grouping bound      |
| `public.length` (if declared) | MUST be ≤ `bounds.maxLength` |

Implicit rule:
- Each Partition represents a single output group.
- Therefore its effective maxNumPartitions = 1.

## 4.5 Contribution Object Constraints (Multiple Privacy Units)

Applied to `csvw-safe:Contribution`.

### 4.5.1 Structural rules
| Rule                                    | Enforcement                                         |
| --------------------------------------- | --------------------------------------------------- |
| MUST declare `contribution.privacyUnit` | Required                                            |
| Referenced column                       | MUST exist in table                                 |
| Referenced column                       | SHOULD be declared as privacy unit in table context |


### 4.5.2 Bound consistency
| Property                        | Constraint           |
| ------------------------------- | -------------------- |
| `contribution.maxContributions` | ≤ parent scope bound |
| `contribution.maxGroupsPerUnit` | ≤ parent scope bound |

If both global bounds and per-contribution bounds exist:
- Per-contribution bounds override for that privacy unit
- Missing bounds inherit from parent scope

## 4.6 Inheritance Rules

Bounds follow hierarchical restriction:
```
Table
  → Column / GroupingKey
      → Partition
          → Contribution
```
Rule: Child scope bounds MUST be ≤ parent scope bounds.
If omitted: Values inherit from parent scope.

## 4.7 DP-Calibratable Metadata Criteria

A dataset is DP-calibratable if:

1. Table declares:
    - public.privacyUnit
    - bounds.maxLength
    - bounds.maxContributions

2. For numeric aggregation columns:
    - minimum
    - maximum

3. For multi-privacy-unit setups:
    - privacyModel
    - Valid Contribution objects

If any required property is missing: DP bounds are undefined.

## 4.8 Additional Validations
- public.length MUST be invariant across neighboring datasets.
- bounds.maxGroupsPerUnit * bounds.maxContributions defines maximum total rows affected per unit.
- bounds.maxNumPartitions constrains vector output size, not sensitivity.
- Partition predicates MUST define disjoint regions if public.exhaustivePartitions = true.

## 4.9 SHACL Enforcement

Some constraints are enforcet in [`csvw-safe-constraints.ttl`](https://github.com/dscc-admin-ch/csvw-safe/blob/main/csvw-safe-library/csvw-safe-constraints.ttl). 
Other more detailed constraints are in [`validate_metadata.py`](https://github.com/dscc-admin-ch/csvw-safe/blob/main/csvw-safe-library/csvw-safe/validate_metadata.py) and [`validate_metadata_shacl.py`](https://github.com/dscc-admin-ch/csvw-safe/blob/main/csvw-safe-library/csvw-safe/validate_metadata_shacl.py) can run the shacl constraints.
