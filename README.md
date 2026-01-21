# CSVW Differential Privacy Extension Vocabulary

## Overview

The **CSVW Differential Privacy Extension (CSVW-DP)** is a vocabulary designed to complement the W3C [CSV on the Web](https://www.w3.org/TR/tabular-data-model/) metadata model.

It defines terms needed to express bounded influence assumptions about individuals in tabular data — the assumptions most differential privacy (DP) systems require but CSVW cannot describe.

See guidelines and notes [here](https://github.com/dscc-admin-ch/csvw-dp/blob/main/guidelines.md).


## Motivation

Differential privacy libraries assume (and often require) metadata such as:

- maximum number of row with the same person
- maximum group sizes
- bounds on how many partitions a person touches  
- constraints that prevent overflow or numerical instability during aggregation

These assumptions are essential for meaningful DP guarantees, but the core CSVW vocabulary cannot express them.

CSVW-DP introduces new terms so that a dataset can explicitly declare:

- table-wide DP contribution bounds  
- per-column DP roles and limits  
- multi-column grouping assumptions

See various libraries and their terms [here](https://github.com/dscc-admin-ch/csvw-dp/blob/main/dp_libraries.md).

---

## Namespace

**Default namespace:** https://github.com/dscc-admin-ch/csvw-dp/csvw-dp-ext#

Machine-readable definitions live in: csvw-dp-ext.ttl

### Table-level properties

| Term | Type | Meaning |
|------|------|---------|
| `dp:maxTableLength` | positive integer | Upper bound on total rows (used to avoid overflow / numerical instability). |
| `dp:tableLength` | positive integer | Number of rows in table (if known). |
| `dp:maxContributions` | positive integer | Max number of rows per individual. |

See [Widespread Underestimation of Sensitivity in Differentially
Private Libraries and How to Fix It](https://dl.acm.org/doi/pdf/10.1145/3548606.3560708) (Casacuberta, Silvia and Shoemate, Michael and Vadhan, Salil and Wagaman, Connor) for `dp:maxTableLength` motivation.


### Column-level properties

| Term | Type | Meaning |
|------|------|---------|
| `dp:privacyId` | boolean | True if the column identifies individuals/units for DP. |
| `dp:nullableProportion` | decimal 0–1 | Fraction of values that are null. |
| `dp:publicPartitions` | list(string) | Declared category set if partitions are known ahead of time. |

`required` in csvw is a boolean for wether the cell must be present and non-empty. 
`dp:nullableProportion` is useful to make more representative dummy dataframe but is optinal (no need to be exact, just coarse idea). 

Specification for datatype: xmlschema seems enough. See point [3 Built-in Datatypes and Their Definitions](https://www.w3.org/TR/xmlschema11-2/). See [datatypes](https://w3c.github.io/csvw/primer/#datatypes).  

If required is false and column has a list of `dp:publicPartitions`, is Nan in `dp:publicPartitions` ? - i think yes by default as it is public?

penguin dataset sex column could be boolean with "format": ("MALE"|"FEMALE") and required=false (for now string). i think it is more a preprocessing issue. in the end it is the same.. a boolean is two categories.


### Groupable

CSVW-DP introduces an abstract helper class: `dp:Groupable`

It represents any entity that can be used to form groups (partitions) for aggregation and differential privacy analysis.

It is not instantiated directly. Instead, two concrete CSVW concepts specialize it:

| Class | Meaning |
|-------|---------|
| `csvw:Column` | A single column used for grouping (one key). |
| `dp:ColumnGroup` | A set of two or more columns grouped collectively (composite key). |


### Per-column group-by differential privacy bounds

(Apply when grouping or aggregating by a single column)

| Term | Type | Meaning |
|------|------|---------|
| `dp:maxPartitionLength` | positive integer | Max size of any group when grouping by the column. |
| `dp:maxNumPartitions` | positive integer | Max number of distinct groups keys. |
| `dp:maxInfluencedPartitions` | positive integer | Max number of groups a person may contribute to. |
| `dp:maxPartitionContribution` | positive integer | Max contributions inside one partition. |



### Multi-column grouping support

CSVW-DP introduces a helper class: `dp:ColumnGroup`

Represents a grouping key formed by two or more columns, or a single column reused in a composite context. Only useful if less privacy spending than worst case of single columns.

| Term | Applies to | Meaning |
|------|------------|---------|
| `dp:columns` | ColumnGroup | List of columns that jointly define the composite key. |
| `dp:maxPartitionLength` | ColumnGroup | Max size of any group when grouping by the columns. |
| `dp:maxNumPartitions` | ColumnGroup | Max number of distinct groups keys. |
| `dp:maxInfluencedPartitions` | ColumnGroup | Max number of groups a person may contribute to. |
| `dp:maxPartitionContribution` | ColumnGroup | Max contributions inside one partition. |

DP properties on dp:ColumnGroup reuse the same terms as columns (dp:maxPartitionLength, etc.), since both are subclasses of dp:Groupable.

`dp:publicPartitions` is not a duplicate of csvw `format` of possible values for string. 
datatype is more the value domain and format explain how to interpret the dp:publicPartitions.
also publicPartitions may be different than actual partitions and the user might agree to spend delta to have more freedom there.

## Diagram

```
csvw:Table
 ├─ dp:maxTableLength        : xsd:positiveInteger
 ├─ dp:tableLength           : xsd:positiveInteger
 └─ dp:maxContributions      : xsd:positiveInteger
        |
        v
 csvw:tableSchema ───────────────────────────────→ csvw:TableSchema
        |
        ├─ csvw:column (0..n) ──────────────────→ csvw:Column
        │                                          ⊂ dp:Groupable
        │        |
        │        ├─ dp:privacyId                : xsd:boolean
        │        ├─ dp:nullableProportion       : xsd:decimal
        │        |
        │        ├─ dp:publicPartitions         : rdf:List
        │        ├─ dp:maxPartitionLength       : xsd:positiveInteger
        │        ├─ dp:maxNumPartitions         : xsd:positiveInteger
        │        ├─ dp:maxInfluencedPartitions  : xsd:positiveInteger
        │        └─ dp:maxPartitionContribution : xsd:positiveInteger
        |
        └─ dp:ColumnGroup (0..n) ───────────────→ dp:ColumnGroup
                                                   ⊂ dp:Groupable
                 |
                 ├─ dp:columns                  : rdf:List (csvw:Column 1..n)
                 ├─ dp:publicPartitions         : rdf:List
                 ├─ dp:maxPartitionLength       : xsd:positiveInteger
                 ├─ dp:maxNumPartitions         : xsd:positiveInteger
                 ├─ dp:maxInfluencedPartitions  : xsd:positiveInteger
                 └─ dp:maxPartitionContribution : xsd:positiveInteger
```


## SHACL Validation Rules

### Nullability
- If `dp:required` is `false`, then `dp:nullableProportion` **MUST be greater than 0** (or not, TODO).
- If `dp:required` is `true`, then `dp:nullableProportion` **MUST be 0** (if `dp:nullableProportion` is provided).


### Privacy ID constraints
- A column with `dp:privacyId = true` **MUST NOT** be groupable.
- A `dp:ColumnGroup` **MUST NOT** include any column where `dp:privacyId = true`.


### Table-level consistency
- If `dp:tableLength` is provided, it **MUST equal** `dp:maxTableLength`.
- `dp:maxPartitionLength` **MUST be less than or equal to** `dp:maxTableLength`.


### Contribution bounds
- `dp:maxInfluencedPartitions` **MUST be less than or equal to** `dp:maxContributions`.
- `dp:maxPartitionContribution` **MUST be less than or equal to** `dp:maxContributions`.


### Public partitions
- If `dp:publicPartitions` is provided:
  - `dp:maxNumPartitions` **MUST equal** the size of `dp:publicPartitions`.
  - Each value in `dp:publicPartitions` **MUST conform to the column’s declared datatype**.


### Multi-column grouping (`dp:ColumnGroup`)
For groupings over multiple columns:

- `dp:maxInfluencedPartitions` **MUST be less than or equal to** `dp:maxContributions`.
- `dp:maxPartitionContribution` **MUST be less than or equal to** `dp:maxContributions`.
- `dp:maxNumPartitions` **MUST be less than or equal to** the product of `dp:maxNumPartitions` of the individual columns.
- `dp:maxPartitionLength` **MUST equal** the minimum `dp:maxPartitionLength` across all grouped columns.


## Theoretical Upper Bounds for `dp:Groupable`

When grouping by multiple columns, it is possible to derive worst-case upper bounds on the resulting partitions from the bounds declared on each individual column.
These derived bounds are always conservative and are safe for differential privacy accounting.

If tighter (less pessimistic) bounds are known, they SHOULD be expressed explicitly using a `dp:ColumnGroup` entry in the metadata.

The following rules describe how bounds may be inferred for a multi-column group-by when no explicit `dp:ColumnGroup` is provided assuming a group-by over columns `C_1, C_2, …, C_n`.

#### `dp:publicPartitions`
- If all grouped columns define `dp:publicPartitions`, the effective public partitions are the Cartesian product of those lists.
- Example:
  - Column A: `["Male", "Female"]`
  - Column B: `["Adelie", "Gentoo", "Chinstrap"]`
  - Derived partitions:
    - `("Male","Adelie")`, `("Male","Gentoo")`, `("Male","Chinstrap")`, `("Female","Adelie")`, `("Female","Gentoo")`, `("Female","Chinstrap")`
- If any grouped column does not declare `dp:publicPartitions`, the composite grouping MUST NOT be treated as public.


#### `dp:maxPartitionLength`
- The upper bound is the minimum of the known `dp:maxPartitionLength` values   across all grouped columns.
- If some columns omit this property, the minimum is taken over the known values.
- Rationale: no composite partition can exceed the smallest contributing bound.

#### `dp:maxNumPartitions`
- The upper bound is the product of each column’s `dp:maxNumPartitions`.
- If any grouped column omits `dp:maxNumPartitions`, the composite bound MUST NOT be inferred.
- Rationale: the number of distinct composite keys grows multiplicatively.

#### `dp:maxInfluencedPartitions`
- The upper bound is the minimum of the known `dp:maxInfluencedPartitions` values.
- If some columns omit this property, the minimum is taken over the known values.
- Rationale: a person cannot influence more composite partitions than the tightest contributing bound.

#### `dp:maxPartitionContribution`
- The upper bound is the minimum of the known `dp:maxPartitionContribution` values.
- If some columns omit this property, the minimum is taken over the known values.
- Rationale: per-partition contribution is constrained by the strictest column.


#### Example
Two columns: `year` and `month`. It is publicly know that data ranges from 06.2026 to 05.2027 and there is one row per day. A person can contribute once per year.
- column `year` has metadata:
    - `dp:publicPartitions`: [2026, 2027]
    - `dp:maxPartitionLength`: 366
    - `dp:maxNumPartitions`: 2
    - `dp:maxInfluencedPartitions`: 2
    - `dp:maxPartitionContribution`: 1
- column `sex` has metadata:
    - `dp:publicPartitions`: [01, 02, 03, 04, 05, 06, 07, 08, 09, 10, 11, 12]
    - `dp:maxPartitionLength`: 31*2 (max days in month * nb year)
    - `dp:maxNumPartitions`: 12
    - `dp:maxInfluencedPartitions`: 2 (2 different month in 2 years)
    - `dp:maxPartitionContribution`: 2 (same month in the 2 years)

In the worst case rules, ColumnGroup [`year`, `month`] has metadata:
- `dp:publicPartitions`: cartesian product of all years and months: all months of 2026 and all months of 2027.
- `dp:maxPartitionLength`: 366 + 365 = 731
- `dp:maxNumPartitions`: 2 * 12=24
- `dp:maxInfluencedPartitions`: 2
- `dp:maxPartitionContribution`: 2

But with domain/data knowledge (if public), ColumnGroup [`year`, `month`] has metadata:
- `dp:publicPartitions`: [06, 07, 08, 09, 10, 11, 12] of 2026 and [01, 02, 03, 04, 05] of 2027.
- `dp:maxPartitionLength`: 366
- `dp:maxNumPartitions`: 12
- `dp:maxInfluencedPartitions`: 1
- `dp:maxPartitionContribution`: 1


## TODOs - WIP

- Make a file for the rules (in SHACL) or pyshacl ? 
- logic for combining continuous columns if binned with known breaks (for lomas feature store potentially, out of scope here)


## Status

It is a work in progress and subject to change.
To be used in Lomas.