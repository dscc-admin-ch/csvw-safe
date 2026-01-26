# CSVW Differential Privacy Extension Vocabulary

## Overview

Differential privacy libraries assume (and often require) metadata such as:

- maximum number of rows contributed by the same person
- maximum group (partition) sizes
- bounds on how many partitions a person can influence
- constraints that prevent overflow or numerical instability during aggregation

These assumptions are essential for meaningful DP guarantees, but the core CSVW vocabulary cannot express them. 

The **CSVW Differential Privacy Extension (CSVW-DP)** is a vocabulary designed to complement the W3C [CSV on the Web](https://www.w3.org/TR/tabular-data-model/) metadata vocabulary. It is a declarative, semantic, DP-aware data modeling system.

It introduces terms needed to explicitly declare contribution bounds at the table, column and multi-column levels —
values that most differential privacy (DP) systems require, but which cannot be expressed using the core CSVW vocabulary alone.

See guidelines and notes [here](https://github.com/dscc-admin-ch/csvw-dp/blob/main/guidelines.md).

We define
1) Vocabulary → What does data mean?
2) Constraints → What values are allowed?

We provide a comprehensive example on the [Penguin dataset]("https://raw.githubusercontent.com/mwaskom/seaborn-data/master/penguins.csv") in [here](https://github.com/dscc-admin-ch/csvw-dp/blob/main/penguin_metadata.yaml).

---

## 1 VOCABULARY

The motivation for the terms was found by looking at what is needed in the various dp libraries and their terms. See [dp_libraries.md](https://github.com/dscc-admin-ch/csvw-dp/blob/main/dp_libraries.md).

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
| `dp:derivedFrom` | csvw:Column | Source column to derive virtual column.. |

Note: 
CSVW's `required` field already expresses whether a cell must be present and non-empty. `dp:nullableProportion` is optional and intended for approximate modeling (e.g., generating representative dummy data or estimating accuracy). Exactness is not required; coarse bounds are sufficient.

`dp:derivedFrom` is better explained at the end of the README.

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

### Diagram
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
        |                      ├─ dp:maxPartitionContribution : xsd:positiveInteger
        |                      ├─ csvw:csvw:virtual           : xsd:boolean
        |                      ├─ dp:derivedFrom              : csvw:Column
        |                      └─ dp:transformationType       : string
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

## 2. CONSTRAINTS

### Worst-Case Bounds for Multi-Column Aggregations (dp:ColumnGroup)
| **Property**                    | **Worst-case bound for grouped columns**                   | **Rule / Derivation**                                                                           | **Notes**                                                                                               |
| ------------------------------- | ---------------------------------------------------------- | ----------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------- |
| **dp:publicPartitions**         | Cartesian product of individual columns’ public partitions | If all grouped columns declare dp:publicPartitions, then group partitions = Cartesian product | If any column does not declare dp:publicPartitions → the group MUST NOT declare dp:publicPartitions |
| **dp:maxPartitionLength**       | `min(dp:maxPartitionLength_i)`                             | Minimum across all grouped columns                                                              | Conservative upper bound; if some columns missing → take min over known values                          |
| **dp:maxNumPartitions**         | `≤ ∏ dp:maxNumPartitions_i`                                | Product of per-column maxima                                                                    | If any column lacks dp:maxNumPartitions → group MUST NOT declare dp:maxNumPartitions                    |
| **dp:maxInfluencedPartitions**  | `min(dp:maxInfluencedPartitions_i)`                        | Minimum of all known column values                                                              | If missing, take min of known values                                                                    |
| **dp:maxPartitionContribution** | `min(dp:maxPartitionContribution_i)`                       | Minimum of all known column values                                                              | If missing, take min of known values                                                                    |

Example for cartesion product of **dp:publicPartitions**:
- Column A: `["Male", "Female"]`
- Column B: `["Adelie", "Gentoo", "Chinstrap"]`
- Derived partitions:
  - `("Male","Adelie")`, `("Male","Gentoo")`, `("Male","Chinstrap")`,
    `("Female","Adelie")`, `("Female","Gentoo")`, `("Female","Chinstrap")`

### Column-Level Structural Constraints
| **Rule**                                                                                                                                                        | **Meaning**                                    |
| --------------------------------------------------------------------------------------------------------------------------------------------------------------- | ---------------------------------------------- |
| If `required = true` → `dp:nullableProportion = 0`                                                                                                              | Required columns cannot be nullable            |
| Column with `dp:privacyId = true` MUST NOT declare: `dp:maxInfluencedPartitions`, `dp:maxPartitionContribution`, `dp:maxNumPartitions`, `dp:maxPartitionLength` | Privacy ID columns cannot be used for bounding |
| If `dp:publicPartitions` is provided → all values must match the declared datatype                                                                              | Type safety constraint                         |


### Multi-Column Group Constraints (dp:ColumnGroup)
| **Rule**                                                                                         | **Meaning**                                       |
| ------------------------------------------------------------------------------------------------ | ------------------------------------------------- |
| Group MUST NOT include any column with `dp:privacyId = true`                                     | Privacy ID columns cannot participate in grouping |
| If any grouped column lacks `dp:publicPartitions` → group MUST NOT declare `dp:publicPartitions` | Prevents unsafe assumptions                       |
| If any grouped column lacks `dp:maxNumPartitions` → group MUST NOT declare `dp:maxNumPartitions` | Avoids underestimating partition count            |


### Table-Level Consistency Rules

| **Property**                                    | **Constraint**                  |
| ----------------------------------------------- | ------------------------------- |
| `dp:tableLength`                                | MUST equal `dp:maxTableLength`  |
| `dp:maxPartitionLength (column or group)`       | ≤ `dp:maxTableLength`           |
| `dp:maxInfluencedPartitions (column or group)`  | ≤ `dp:maxContributions (table)` |
| `dp:maxPartitionContribution (column or group)` | ≤ `dp:maxContributions (table)` |



### Resulting Validation Rules

SHACL file for enforcing constraints in [constraints_shacl.ttl](https://github.com/dscc-admin-ch/csvw-dp/blob/main/constraints_shacl.ttl).


-------------------------------------------------------

## Theoretical Upper Bounds for `dp:Groupable`

If tighter (less pessimistic) bounds are known than the worst case enforced by the validation file, they SHOULD be expressed explicitly using a `dp:ColumnGroup` entry in the metadata.

For example, with two columns: `year` and `month`. It is publicly know that data ranges from 06.2026 to 05.2027 and there is one row per day. A person can contribute once per year.
- column `year` has metadata:
    - `dp:publicPartitions`: [2026, 2027]
    - `dp:maxPartitionLength`: 366
    - `dp:maxNumPartitions`: 2
    - `dp:maxInfluencedPartitions`: 2
    - `dp:maxPartitionContribution`: 1
- column `month` has metadata:
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

---

## Logic for derived columns
In many data processing pipelines, preprocessing steps such as filtering, binning, clipping, truncation, and recoding are applied before differential privacy (DP) mechanisms. These transformations often lead to tighter contribution bounds and sensitivity limits than those implied by the raw data.

For example:

- In OpenDP, filtering introduces conservative slack in sensitivity estimates. If tighter bounds are known, preserving them can significantly improve utility.
- Binning is frequently used to convert continuous variables into categorical ones (e.g., age groups, time buckets, or synthetic data generation), and domain knowledge may allow the declaration of precise public partitions.

To support these cases, CSVW-DP leverages CSVW virtual columns, allowing derived data to be described declaratively in metadata while attaching refined DP bounds.

#### Virtual Columns in CSVW

CSVW supports virtual columns, i.e., columns that do not exist physically in the CSV file but are defined by transformation expressions in metadata.

Virtual columns can represent preprocessing steps such as filtering, binning, truncation, clipping, recoding, mapping, etc.

They are declared using:
```
"virtual": true,
"valueUrl": "... expression ..."
```
and in this DP extension should declare their source columns using:
```
dp:derivedFrom : csvw:Column
```

CSVW-DP reuses all DP properties on virtual columns exactly as on physical columns, allowing tighter post-transformation bounds to be expressed without introducing new DP-specific transformation primitives.

#### Common Transformations and Their DP Effects

Derived columns may declare the transformation category using:
```
dp:transformationType ∈ {filter, bin, clip, truncate, recode}
```

| Operation  | Effect                                                       | Canonical Form                |
| ---------- | ------------------------------------------------------------ | ------------------------------|
| filter     | reduces `dp:maxTableLength`, `dp:maxContributions`           | `filter(col, predicate)`      |
| binning    | reduces `dp:maxNumPartitions`, defines `dp:publicPartitions` | `bin(col, min, max, width)`   |
| clipping   | tightens `minimum` / `maximum`                               | `clip(col, lower, upper)`     |
| truncation | tightens per-individual contribution bounds                  | `truncate(col, max_rows)`     |
| recoding   | shrinks categorical universe                                 | `recode(col, mapping)`        |
| concatenation | -                                                         | `concatenation(col_1, col_2)` |
| one-hot encoding | -                                                      | `onehot(col)`                 |

###### Examples: 
**Binning**
Raw
```
"name": "age",
"datatype": "integer",
"minimum": 0,
"maximum": 120
```

Derived
```
"name": "age_bin_0_120_10",
"dp:derivedFrom": ["age"]
"virtual": true,
"valueUrl": "bin(age, 0, 120, 10)",
"datatype": "string",
"format": "(0-9|10-19|...|110-119|120+)",
"dp:publicPartitions": ["0-9","10-19","20-29",...,"120+"],
"dp:maxNumPartitions": 13,
"dp:maxPartitionLength": 100000,
"dp:maxInfluencedPartitions": 1,
"dp:maxPartitionContribution": 1
```

**Filtering**
Filtering reduces dataset size and per-person contribution bounds.

Raw
```
"name": "days",
dp:maxTableLength = 1_000_000
dp:maxContributions = 365
```

Derived
```
"name": "days_filtered_6",
"virtual": true,
"dp:derivedFrom": ["days"],
"valueUrl": "filter(days, month == 6)",
"dp:maxTableLength": 30000,
"dp:maxContributions": 30
```


**Clipping**
Clipping limits numeric ranges to reduce sensitivity.

Raw
```
"name": "salary",
"datatype": "integer",
"minimum": 0,
"maximum": 10000000
```

Derived
```
"name": "salary_clipped_0_200000",
"virtual": true,
"dp:derivedFrom": ["salary"],
"valueUrl": "clip(salary, 0, 200000)",
"minimum": 0,
"maximum": 200000
```

**Truncating**
Truncation enforces per-individual contribution caps at the preprocessing level.

Raw
```
"name": "events",
"dp:maxContributions": 1000
```

Derived
```
"name": "events_truncated_100",
"virtual": true,
"dp:derivedFrom": ["events"],
"valueUrl": "truncate(events, 100)",
"dp:maxContributions": 100
```

**Recoding**
Recoding collapses or maps categories to a smaller public universe.

Raw
```
"name": "occupation",
"datatype": "string",
"format": "string"
```

Derived
```
"name": "occupation_grouped_education_healthcare_other",
"virtual": true,
"dp:derivedFrom": ["occupation"],
"valueUrl": "recode(occupation, {teacher, professor -> education; nurse, doctor -> healthcare; * -> other})",
"datatype": "string",
"dp:publicPartitions": ["education", "healthcare", "other"],
"dp:maxNumPartitions": 3
```
