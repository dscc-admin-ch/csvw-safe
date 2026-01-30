# CSVW Differential Privacy Extension Vocabulary

### 1.7 Full Visual Overview
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

## 2. Constraints details

### 2.1 Worst-Case Bounds for Multi-Column Aggregations

Example for cartesion product of **dp:publicPartitions**:
- Column A: `["Male", "Female"]`
- Column B: `["Adelie", "Gentoo", "Chinstrap"]`
- Derived partitions:
  - `("Male","Adelie")`, `("Male","Gentoo")`, `("Male","Chinstrap")`,
    `("Female","Adelie")`, `("Female","Gentoo")`, `("Female","Chinstrap")`

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

### 2.6 All constraints

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

