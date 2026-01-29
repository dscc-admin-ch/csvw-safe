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
| `dp:groupable` | boolean | True if the column is groupable. |
| `dp:nullableProportion` | decimal 0–1 | Fraction of values that are null. |
| `dp:publicPartitions` | list(string) | Declared set of partition keys when grouping values are publicly known. |

Note: 
CSVW's `required` field already expresses whether a cell must be present and non-empty. `dp:nullableProportion` is optional and intended for approximate modeling (e.g., generating representative dummy data or estimating accuracy). Exactness is not required; coarse bounds are sufficient.

`dp:groupable` boolean does it clash with class `dp:Groupable` ? 

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

#### Derived columns (preprocessing) privacy bounds (WIP)

CSVW-DP introduces a new class: `dp:DerivedColumn rdfs:subClassOf csvw:Column .`

To identify these columns, we add `csvw:virtual = True`. 
This makes the column non-physical (not present in raw CSV) but part of the logical schema.

| Term | Type | Meaning |
|------|------|---------|
| `csvw:virtual` | boolean| True if column is derived/preprocessed and not present in the raw input. |
| `dp:derivedFrom` | [string] | Source column(s) used to compute this column. |
| `dp:transformationType` | `dp:Transformation` | Identifier of the preprocessing operation. |
| `dp:transformationArguments` | object | Parameters of the transformation. |

Can give private contributions if better than worst case based on source column. Otherwise, will be computed based on worst case.

Design choice: `dp:derivedFrom` should always be a list, even if length = 1, to keep the model uniform.

In Lomas, can give `get_dummy(with_virtual=True)` with virtual columns.
More on derived columns below (enabled `dp:Transformation`, their associated `dp:transformationArguments` and examples).

---

### Diagram
```
csvw:Table
 ├─ dp:maxTableLength
 ├─ dp:tableLength
 ├─ dp:maxContributions
 │
 ├─ csvw:tableSchema ──────────────→ csvw:TableSchema
 │        |
 │        └─ csvw:column ──────────→ csvw:Column ⊂ dp:Groupable
 │                   |
 │                   ├─ core CSVW schema
 │                   ├─ DP group-by bounds
 │                   └─ derived-column semantics
 │
 └─ dp:ColumnGroup ───────────────→ dp:ColumnGroup ⊂ dp:Groupable
             |
             └─ multi-column DP bounds
```

In full:
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
        |                      ├─ dp:transformationType       : string
        |                      └─ dp:transformationArguments  : object
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

SHACL file for enforcing constraints in [metadata_constraints.ttl](https://github.com/dscc-admin-ch/csvw-dp/blob/main/metadata_constraints.ttl).

| Scope                 | Property                                                   | Constraint / Rule                            | Worst-Case Check                                        | Comments                                                        |
| --------------------- | ---------------------------------------------------------- | -------------------------------------------- | ------------------------------------------------------- | --------------------------------------------------------------- |
| **Table**             | `dp:maxTableLength`                                        | Must be positive integer                     | N/A                                                     | Upper bound on total rows to prevent overflow in DP aggregation |
|                       | `dp:tableLength`                                           | Positive integer, ≤ `dp:maxTableLength`      | ✅                                                       | Actual number of rows must not exceed declared maximum          |
|                       | `dp:maxContributions`                                      | Positive integer, ≤ `dp:maxTableLength`      | ✅                                                       | Max rows per person cannot exceed table max rows                |
|                       | `csvw:tableSchema`                                         | Must exist                                   | N/A                                                     | Table must declare schema                                       |
| **Column**            | `dp:privacyId`                                             | Boolean                                      | Must not declare DP bounds (`maxPartitionLength`, etc.) | True if column identifies individuals, cannot be grouped        |
|                       | `dp:groupable`                                             | Boolean                                      | N/A                                                     | Indicates if column can be used for DP groupings                |
|                       | `dp:nullableProportion`                                    | Decimal [0,1]                                | N/A                                                     | Fraction of nulls                                               |
|                       | `dp:publicPartitions`                                      | List                                         | Values must match datatype                              | Declared universe of partition keys                             |
|                       | `dp:maxPartitionLength`                                    | Positive integer                             | ≤ table `dp:maxTableLength`                             | Max size of a group for this column                             |
|                       | `dp:maxNumPartitions`                                      | Positive integer                             | ≤ table `dp:maxTableLength`                             | Max number of distinct groups                                   |
|                       | `dp:maxInfluencedPartitions`                               | Positive integer                             | ≤ table `dp:maxContributions`                           | Max groups a person can contribute to                           |
|                       | `dp:maxPartitionContribution`                              | Positive integer                             | ≤ table `dp:maxContributions`                           | Max contributions per partition                                 |
| **Derived Column**    | `csvw:virtual`                                             | Must be true                                 | N/A                                                     | Marks column as virtual, not present in raw CSV                 |
|                       | `dp:derivedFrom`                                           | ≥1 source column                             | N/A                                                     | Must reference one or more source columns                       |
|                       | `dp:transformationType`                                    | String                                       | N/A                                                     | Type of preprocessing/transformation                            |
|                       | `dp:transformationArguments`                               | Optional object                              | N/A                                                     | Parameters of transformation                                    |
|                       | DP bounds (`maxPartitionLength`, etc.)                     | ≤ source columns & table worst-case          | ✅                                                       | Ensures derived columns do not exceed worst-case DP bounds      |
| **ColumnGroup**       | `dp:columns`                                               | ≥2 columns                                   | N/A                                                     | Must include multiple columns                                   |
|                       | DP bounds (`maxPartitionLength`, `maxNumPartitions`, etc.) | ≤ min/product of constituent columns & table | ✅                                                       | Worst-case enforcement across group columns                     |
|                       | PrivacyId columns                                          | Must not be included                         | ✅                                                       | PrivacyId columns cannot participate in groups                  |
| **Required Columns**  | `csvw:required`                                            | If true → `dp:nullableProportion = 0`        | ✅                                                       | Enforces non-nullability aligns with CSVW rules                 |
| **Public Partitions** | `dp:publicPartitions`                                      | All values must match column datatype        | N/A                                                     | Type safety for partition keys                                  |

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
dp:transformationType ∈ {filter, bin, clip, truncate, recode, concatenation, onehot}
```

| Operation  | Effect                                                       | Canonical Form                |
| ---------- | ------------------------------------------------------------ | ------------------------------|
| clipping   | tightens `minimum` / `maximum`                               | `clip(col, lower, upper)`     |
| truncation | tightens per-individual contribution bounds                  | `truncate(col, max_rows)`     |
| fill_na_constant | replaces missing values with a public constant         | `fill_na_constant(col, val)`  |
| fill_na_data_derived | replaces missing values data-derived value (mean/median/mode) | `fill_na_data_derived(col, func)`  |
| filter     | reduces `dp:maxTableLength`, `dp:maxContributions`           | `filter(col, predicate)`      |
| binning    | reduces `dp:maxNumPartitions`, defines `dp:publicPartitions` | `bin(col, min, max, width)`   |
| recoding   | shrinks categorical universe                                 | `recode(col, mapping)`        |
| concatenation | combines multiple columns into a composite categorical domain | `concatenation(col_1, col_2, ...)` |
| one-hot encoding | expands a categorical column into binary indicator columns | `onehot(col)`                 |

| Transformation | Cardinality (per column) | Row Count     | Partition Count   | Privacy Impact                      |
| ---------------| ------------------------ | ------------- | ----------------- | ----------------------------------- |
| clipping       | same                     | same          | same              | ↓ sensitivity (bounds tightening)   |
| truncation     | same                     | same          | same              | ↓ per-user influence                |
| fill_na_constant | same                   | same          | same              | neutral                             |
| fill_na_data_derived | same               | same          | same              | ↑ sensitivity                       |
| filter         | same                     | **decreases** | same              | ↓ sensitivity, ↓ total contribution |
| binning        | **decreases**            | same          | **decreases**     | ↓ sensitivity, ↓ partition leakage  |
| recoding       | **decreases**            | same          | **decreases**     | ↓ sensitivity, ↓ domain leakage     |
| concatenation  | **increases**            | same          | **increases**     | ↑ sensitivity                       |
| onehot         | expands columns          | same          | constant (=2)     | ↑ dimensionality                    |


| Rank    | Transformation       | Notes on DP Risk                                           |
| ------- | -------------------- | ---------------------------------------------------------- |
| 1 Best  | truncation           | caps per-user rows → very safe                             |
| 1       | clipping             | tightens value bounds → very safe                          |
| 1       | fill_na_constant     | fills missing with public constant → neutral               |
| 2       | binning              | reduces domain size → moderate safety improvement          |
| 2       | recoding             | reduces categorical universe → moderate safety improvement |
| 3       | filter               | removes rows, reduces contributions but might single out   |
| 3       | concatenation        | multiplies domain → increased sensitivity                  |
| 3       | onehot               | expands columns → increases dimensionality                 |
| 4 Worst | fill_na_data_derived | injects data-dependent value → increased sensitivity       |


TODO: list of `dp:Transformation` and their associated `dp:transformationArguments`. 
TODO: see if something already exist.

###### Examples: 
**Binning**

From
```
"user_id" | "age"
----------------------------
1         | 5
1         | 6
2         | 19
2         | 21
3         | 38
```
to (with `bin(age, 0, 120, 10)`)
```
"user_id" | "bin_age_0_120_10" |
--------------------------------
1         | (0-9               |
1         | (0-9               |
2         | (10-19             |
2         | (20-29             |
3         | (30-28             |
```

Raw
```
"name": "age",
"datatype": "integer",
"minimum": 0,
"maximum": 120
```

Derived
```
"name": "bin_age_0_120_10",
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

From
```
"user_id" | "date"
--------------------
1         | 2025-06-01
1         | 2025-07-02
2         | 2025-06-15
2         | 2025-08-03
3         | 2025-06-20
```
to (with `filter(date, month == 6)`)
```
"user_id" | "filter_days_6"
--------------------
1         | 2025-06-01
2         | 2025-06-15
3         | 2025-06-20
```

Raw
```
"name": "days",
dp:maxTableLength = 1_000_000
dp:maxContributions = 365
```

Derived
```
"name": "filter_days_6",
"virtual": true,
"dp:derivedFrom": ["days"],
"valueUrl": "filter(days, month == 6)",
"dp:maxTableLength": 30000,
"dp:maxContributions": 30
```


**Clipping**
Clipping limits numeric ranges to reduce sensitivity.

From
```
"user_id" | "salary"
---------------------
1         | 180000
2         | 250000
3         | 75000
4         | 5000000
```
to (with `clip(salary, 0, 200000)`)
```
"user_id" | "clip_salary_0_200000"
-----------------------------------
1         | 180000
2         | 200000
3         | 75000
4         | 200000
```

Raw
```
"name": "salary",
"datatype": "integer",
"minimum": 0,
"maximum": 10000000
```

Derived
```
"name": "clip_salary_0_200000",
"virtual": true,
"dp:derivedFrom": ["salary"],
"valueUrl": "clip(salary, 0, 200000)",
"minimum": 0,
"maximum": 200000
```

**Truncating**
Truncation enforces per-individual contribution caps at the preprocessing level.

From
```
"user_id" | "event_id"
-----------------------
1         | 1
1         | 2
...
1         | 1000
2         | 1
2         | 2
...
2         | 1000
```
to (with `truncate(events, 100)`)
```
"user_id" | "event_id"
-----------------------
1         | 1
1         | 2
...
1         | 100
2         | 1
2         | 2
...
2         | 100
```

Raw
```
"name": "events",
"dp:maxContributions": 1000
```

Derived
```
"name": "truncate_events_100",
"virtual": true,
"dp:derivedFrom": ["events"],
"valueUrl": "truncate(events, 100)",
"dp:maxContributions": 100
```

**Recoding**
Recoding collapses or maps categories to a smaller public universe.

From
```
"user_id" | "occupation"
-------------------------
1         | teacher
2         | doctor
3         | taxi_driver
4         | nurse
5         | professor
```
to (with `recode(occupation, {...})`)
```
"user_id" | "recode_occupation_education_healthcare_other"
-----------------------------------------------------------
1         | education
2         | healthcare
3         | other
4         | healthcare
5         | education
```

Raw
```
"name": "occupation",
"datatype": "string",
"format": "string"
```

Derived
```
"name": "recode_occupation_education_healthcare_other",
"virtual": true,
"dp:derivedFrom": ["occupation"],
"valueUrl": "recode(occupation, {teacher, professor -> education; nurse, doctor -> healthcare; * -> other})",
"datatype": "string",
"dp:publicPartitions": ["education", "healthcare", "other"],
"dp:maxNumPartitions": 3
```

**Concatenation**
Recoding collapses or maps categories to a smaller public universe.

From
```
"user_id" | "is_before_1_august" | "planned_caesarean"
--------------------------------------------------------
1         | True                 | False
2         | False                | False
3         | True                 | True
4         | False                | True
```
to (with `concatenation(is_before_1_august, planned_caesarean)`)
```
"user_id" | "concat_is_before_1_august_planned_caesarean"
-----------------------------------------------------------
1         | True_False
2         | False_False
3         | True_True
4         | False_True
```

Raw
```
"name": "is_before_1_august",
"datatype": "boolean"
```
and
```
"name": "planned_caesarean",
"datatype": "boolean"
```

Derived
```
"name": "concat_is_before_1_august_planned_caesarean",
"dp:derivedFrom": ["is_before_1_august", "planned_caesarean"],
"virtual": true,
"valueUrl": "concatenation(is_before_1_august, planned_caesarean)",
"datatype": "string",
"format": "(True|False)_(True|False)",
"dp:publicPartitions": [
  "True_True",
  "True_False",
  "False_True",
  "False_False"
],
"dp:maxNumPartitions": 4,
"dp:maxPartitionLength": 100000,
"dp:maxInfluencedPartitions": 1,
"dp:maxPartitionContribution": 1
```

**One-Hot Encoding**
Recoding collapses or maps categories to a smaller public universe.

From
```
"user_id" | "delivery_mode"
----------------------------
1         | spontaneous
1         | planned_cesarean
1         | spontaneous
2         | spontaneous
2         | emergency_cesarean
```
to (with `onehot(delivery_mode)`)
```
"user_id" | "one_hot_delivery_mode_spontaneous" | "one_hot_delivery_mode_planned_cesarean" | "one_hot_delivery_mode_emergency_cesarean"
------------------------------------------------------------------------
1         | True          | False              | False
1         | False         | True               | False
1         | True          | False              | False
2         | True          | False              | False
2         | False         | False              | True
```

Raw
```
"name": "delivery_mode",
"datatype": "string",
"format": "(spontaneous|planned_cesarean|emergency_cesarean)"
```

Derived
```
"name": "one_hot_delivery_mode_spontaneous",
"dp:derivedFrom": ["delivery_mode"],
"virtual": true,
"valueUrl": "onehot(delivery_mode)",
"datatype": "boolean",
"dp:publicPartitions": ["True", "False"],
"dp:maxNumPartitions": 2,
"dp:maxPartitionLength": 100000,
"dp:maxInfluencedPartitions": 1,
"dp:maxPartitionContribution": 1
```
and
```
"name": "one_hot_delivery_mode_planned_cesarean",
"dp:derivedFrom": ["delivery_mode"],
"virtual": true,
"valueUrl": "onehot(delivery_mode)",
"datatype": "boolean",
"dp:publicPartitions": ["True", "False"]
```
and
```
"name": "one_hot_delivery_mode_emergency_cesarean",
"dp:derivedFrom": ["delivery_mode"],
"virtual": true,
"valueUrl": "onehot(delivery_mode)",
"datatype": "boolean",
"dp:publicPartitions": ["True", "False"]
```

