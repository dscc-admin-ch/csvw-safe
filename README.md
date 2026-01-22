# CSVW Differential Privacy Extension Vocabulary

## Overview

The **CSVW Differential Privacy Extension (CSVW-DP)** is a vocabulary designed to complement the W3C [CSV on the Web](https://www.w3.org/TR/tabular-data-model/) metadata model.

It introduces terms needed to express bounded individual influence assumptions about tabular data —
assumptions that most differential privacy (DP) systems require, but which cannot be expressed using the core CSVW vocabulary alone.

See guidelines and notes [here](https://github.com/dscc-admin-ch/csvw-dp/blob/main/guidelines.md).


## Motivation

Differential privacy libraries assume (and often require) metadata such as:

- maximum number of rows contributed by the same person
- maximum group (partition) sizes
- bounds on how many partitions a person can influence
- constraints that prevent overflow or numerical instability during aggregation

These assumptions are essential for meaningful DP guarantees, but the core CSVW vocabulary cannot express them.

CSVW-DP introduces new terms so that a dataset can explicitly declare contribution bounds at the table, column and multi-column levels.

The motivation for the terms was found by looking at what is needed in the various dp libraries and their terms. See [dp_libraries.md](https://github.com/dscc-admin-ch/csvw-dp/blob/main/dp_libraries.md).

---

## Namespace

**Default namespace:** https://github.com/dscc-admin-ch/csvw-dp/csvw-dp-ext#

Machine-readable definitions live in: csvw-dp-ext.ttl

### Table-level properties

| Term | Type | Meaning |
|------|------|---------|
| `dp:maxTableLength` | positive integer | Upper bound on total rows (used to avoid overflow / numerical instability). |
| `dp:tableLength` | positive integer | Number of rows in table (if known). |
| `dp:maxContributions` | positive integer | Maximum number of rows contributed by a single individual. |

The motivation for `dp:maxTableLength` is discussed in  [Widespread Underestimation of Sensitivity in Differentially
Private Libraries and How to Fix It](https://dl.acm.org/doi/pdf/10.1145/3548606.3560708) (Casacuberta et al., 2022).

CSVW’s existing `csvw:tableSchema` remains responsible for defining the table structure.


### Column-level properties

| Term | Type | Meaning |
|------|------|---------|
| `dp:privacyId` | boolean | True if the column identifies individuals/units for DP. |
| `dp:nullableProportion` | decimal 0–1 | Fraction of values that are null. |
| `dp:publicPartitions` | list(string) | Declared set of partition keys when grouping values are publicly known. |

Note: 
CSVW's `required` field already expresses whether a cell must be present and non-empty. `dp:nullableProportion` is optional and intended for approximate modeling (e.g., generating representative dummy data or estimating accuracy). Exactness is not required; coarse bounds are sufficient.

#### Reuse of existing CSVW terms:
CSVW-DP intentionally reuses existing CSVW vocabulary where semantics already exist.
- datatype: Defines the value domain (XML Schema datatypes are sufficient). Specs [here](https://www.w3.org/TR/xmlschema11-2/) and [here](https://w3c.github.io/csvw/primer/#datatypes).
- format: Further constrains allowed representations (e.g. strings for categories). More details on this with `dp:publicPartitions`
- minimum: Lower bound for sensitivity and clipping.
- maximum: Upper bound for sensitivity and clipping.
- required: Indicates whether null values are allowed.
- default: Default value used for missing data.

TODO: If required is false and column has a list of `dp:publicPartitions`, is Nan in `dp:publicPartitions` ? - i think yes by default as it is public?
penguin dataset sex column could be boolean with "format": ("MALE"|"FEMALE") and required=false (for now string). i think it is more a preprocessing issue. in the end it is the same.. a boolean is two categories.


### Groupable

CSVW-DP introduces an abstract helper class: `dp:Groupable`

It represents any entity that can be used to form groups (partitions) for aggregation and differential privacy analysis.

It is not instantiated directly. Instead, two concrete CSVW concepts specialize it:

| Class | Meaning |
|-------|---------|
| `csvw:Column` | A single column used for grouping (one key). |
| `dp:ColumnGroup` | A set of two or more columns grouped collectively (composite key). |

#### Per-column group-by differential privacy bounds

Apply when grouping or aggregating by a single column

| Term | Type | Meaning |
|------|------|---------|
| `dp:maxPartitionLength` | positive integer | Max size of any group when grouping by the column. |
| `dp:maxNumPartitions` | positive integer | Max number of distinct groups keys. |
| `dp:maxInfluencedPartitions` | positive integer | Max number of groups a person may contribute to. |
| `dp:maxPartitionContribution` | positive integer | Max contributions inside one partition. |


`dp:maxNumPartitions` is not necessarily equal to the length of `dp:publicPartitions` when they are provided because some partitions might not be public.


#### Multi-column grouping support

CSVW-DP introduces a helper class: `dp:ColumnGroup`

Represents a grouping key formed by two or more columns. 
It is useful when domain knowledge allows tighter bounds than the worst case of individual columns composition.

| Term                          | Meaning |
|-------------------------------|---------|
| `dp:columns`                  | List of columns that jointly define the composite key. |
| `dp:maxPartitionLength`       | Max size of any group when grouping by the columns. |
| `dp:maxNumPartitions`         | Max number of distinct groups keys. |
| `dp:maxInfluencedPartitions`  | Max number of groups a person may contribute to. |
| `dp:maxPartitionContribution` | Max contributions inside one partition. |

DP properties on `dp:ColumnGroup` reuse the same terms as columns (`dp:maxPartitionLength`, etc.), since both are subclasses of `dp:Groupable`.


**publicPartitions vs format**: 
`datatype` and `format` describe the value domain while `dp:publicPartitions` describes the grouping universe used for DP accounting.
Thus, `dp:publicPartitions` may be larger or smaller than observed values. It may include values not present in the data and declaring publicPartitions can reduce the sensitivity and avoid worst-case assumptions. Users may still choose to spend privacy budget (e.g. δ) to explore unknown partitions.
If `required = false` and `dp:publicPartitions` is present, the partition of missing values (e.g. NaN) is assumed to be publicly known.

---

## Diagram
```
csvw:Table
 ├─ csvw:tableSchema → csvw:TableSchema
 │      └─ csvw:column (0..n)
 └─ dp:ColumnGroup (0..n)
        └─ dp:columns → csvw:Column (1..n)
```

```
csvw:Table
 ├─ dp:maxTableLength        : xsd:positiveInteger
 ├─ dp:tableLength           : xsd:positiveInteger
 └─ dp:maxContributions      : xsd:positiveInteger
        |
        +── csvw:tableSchema ───────────────────────────────→ csvw:TableSchema
        |          |
        |          └─ csvw:column (0..n) ──────────────────→ csvw:Column ⊂ dp:Groupable
        |                      |
        |                      ├─ datatype                   : xsd:anySimpleType
        |                      ├─ format                     : string
        |                      ├─ minimum                    : xsd:decimal
        |                      ├─ maximum                    : xsd:decimal
        |                      ├─ required                   : xsd:boolean
        |                      ├─ default                    : any
        |                      ├─ dp:privacyId                : xsd:boolean
        |                      ├─ dp:nullableProportion       : xsd:decimal
        |                      ├─ dp:publicPartitions         : rdf:List
        |                      ├─ dp:maxPartitionLength       : xsd:positiveInteger
        |                      ├─ dp:maxNumPartitions         : xsd:positiveInteger
        |                      ├─ dp:maxInfluencedPartitions  : xsd:positiveInteger
        |                      └─ dp:maxPartitionContribution : xsd:positiveInteger
        |
        +── dp:ColumnGroup (0..n) ───────────────────────────→ dp:ColumnGroup ⊂ dp:Groupable
                   |
                   ├─ dp:columns                  : rdf:List (csvw:Column 1..n)
                   ├─ dp:publicPartitions         : rdf:List
                   ├─ dp:maxPartitionLength       : xsd:positiveInteger
                   ├─ dp:maxNumPartitions         : xsd:positiveInteger
                   ├─ dp:maxInfluencedPartitions  : xsd:positiveInteger
                   └─ dp:maxPartitionContribution : xsd:positiveInteger
```
---

## Validation Rules

SHACL file for enforcing constraints in [constraints_shacl.ttl](https://github.com/dscc-admin-ch/csvw-dp/blob/main/constraints_shacl.ttl).

### Column-level
- If `required` is `true`, then `dp:nullableProportion` **MUST be 0** (if `dp:nullableProportion` is provided).

- A column with `dp:privacyId = true` **MUST NOT** cannot have fields `dp:maxInfluencedPartitions`, `dp:maxPartitionContribution`, `dp:maxNumPartitions`, `dp:maxPartitionLength`.

- If `dp:publicPartitions` is provided:
  - Each value in `dp:publicPartitions` **MUST conform to the column’s declared datatype**.


### Multi-column level (`dp:ColumnGroup`)

- A `dp:columns` in`dp:ColumnGroup` **MUST NOT** include any column where `dp:privacyId = true`.
- If any `dp:columns` in`dp:ColumnGroup` does not declare `dp:publicPartitions`, the composite grouping **MUST NOT** any declare `dp:publicPartitions`.
- If any `dp:columns` in`dp:ColumnGroup` does not declare `dp:maxNumPartitions`, the composite grouping **MUST NOT** any declare `dp:maxNumPartitions`.

When grouping by multiple columns, it is possible to derive worst-case upper bounds on the resulting partitions from the bounds declared on each individual column.
These derived bounds are always conservative and are safe for differential privacy accounting.
If bounds are larger then there is an error.

- If all grouped columns define `dp:publicPartitions`, the public partitions **MUST be** the Cartesian product of those lists.
- Example:
  - Column A: `["Male", "Female"]`
  - Column B: `["Adelie", "Gentoo", "Chinstrap"]`
  - Derived partitions:
    - `("Male","Adelie")`, `("Male","Gentoo")`, `("Male","Chinstrap")`, `("Female","Adelie")`, `("Female","Gentoo")`, `("Female","Chinstrap")`

- `dp:maxPartitionLength` of a group of columns **MUST equal** the minimum `dp:maxPartitionLength` across all individual columns.


- `dp:maxNumPartitions` of a group of columns **MUST be less than or equal to** the product of `dp:maxNumPartitions` of all the individual columns.

- The upper bound is the minimum of the known `dp:maxInfluencedPartitions` values.


- The upper bound is the minimum of the known `dp:maxPartitionContribution` values.

**Note**: for allowed aggregations with missing columns:
- If any `dp:columns` in`dp:ColumnGroup` does not declare `dp:maxPartitionLength`, the minimum **SHOULD** be taken over the known values.
- If any `dp:columns` in`dp:ColumnGroup` does not declare `dp:maxInfluencedPartitions`, the minimum **SHOULD** be taken over the known values.
- If any `dp:columns` in`dp:ColumnGroup` does not declare `dp:maxPartitionContribution`, the minimum **SHOULD** be taken over the known values.


### Table-level consistency
- If `dp:tableLength` is provided, it **MUST equal** `dp:maxTableLength`.

- `dp:maxPartitionLength` of a column or of a group of columns **MUST be less than or equal to** `dp:maxTableLength` of the table.

- `dp:maxInfluencedPartitions` of a column or of a group of columns **MUST be less than or equal to** `dp:maxContributions` of the table.

- `dp:maxPartitionContribution` of a column or of a group of columns **MUST be less than or equal to** `dp:maxContributions` of the table.


---

## Theoretical Upper Bounds for `dp:Groupable`

If tighter (less pessimistic) bounds are known than the worst case enforced by the validation file, they SHOULD be expressed explicitly using a `dp:ColumnGroup` entry in the metadata.

For example, with two columns: `year` and `month`. It is publicly know that data ranges from 06.2026 to 05.2027 and there is one row per day. A person can contribute once per year.
- column `year` has metadata:
    - `dp:publicPartitions`: [2026, 2027]
    - `dp:maxPartitionLength`: 366
    - `dp:maxNumPartitions`: 2
    - `dp:maxInfluencedPartitions`: 2
    - `dp:maxPartitionContribution`: 1
- column `sex` has metadata:
    - `dp:publicPartitions`: [01, 02, 03, 04, 05, 06, 07, 08, 09, 10, 11, 12]
    - `dp:maxPartitionLength`: 31 (max days in months)
    - `dp:maxNumPartitions`: 12
    - `dp:maxInfluencedPartitions`: 2 (2 different month in 2 years)
    - `dp:maxPartitionContribution`: 2 (same month in the 2 years)

In the worst case rules, ColumnGroup [`year`, `month`] has metadata:
- `dp:publicPartitions`: cartesian product of all years and months: all months of 2026 and all months of 2027.
- `dp:maxPartitionLength`: min(366, 31) = 31
- `dp:maxNumPartitions`: 2 * 12 = 24
- `dp:maxInfluencedPartitions`: min(2, 2) = 2
- `dp:maxPartitionContribution`: min(1, 2) = 1

But with domain/data knowledge (if public), ColumnGroup [`year`, `month`] has metadata:
- `dp:publicPartitions`: [06, 07, 08, 09, 10, 11, 12] of 2026 and [01, 02, 03, 04, 05] of 2027.
- `dp:maxPartitionLength`: 31
- `dp:maxNumPartitions`: 12
- `dp:maxInfluencedPartitions`: 1
- `dp:maxPartitionContribution`: 1


## TODOs - WIP
- logic for combining continuous columns if binned with known breaks (for lomas feature store potentially, out of scope here)

