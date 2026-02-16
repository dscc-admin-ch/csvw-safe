# CSVW Differential Privacy Extension Vocabulary

### 1.7 Full Visual Overview

In full:
```
Table
 ├─ Table Properties
 │   ├─ maxLength (required)
 │   ├─ publicLength
 │   └─ contributions
 │
 └─ Schema
     ├─ Columns
     │   └─ Column
     │       ├─ Column Properties
     │       ├─ Groupable Properties
     │       └─ publicPartitions
     │           └─ Partition
     │               ├─ PartitionKey
     │               └─ Partition Properties
     │
     └─ ColumnGroups
         └─ ColumnGroup
             ├─ columns (list of column references)
             ├─ Groupable Properties
             └─ publicPartitions
                 └─ Partition
                     ├─ components
                     │   └─ PartitionKey (per column)
                     └─ Partition Properties
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
    - `dp:maxPartitionContribution`: 1

In the worst case rules, ColumnGroup [`year`, `month`] has metadata:
- `dp:publicPartitions`: cartesian product of all years and months: all months of 2026 and all months of 2027.
- `dp:maxPartitionLength`: min(366, 31) = 31
- `dp:maxNumPartitions`: 2 * 12 = 24
- `dp:maxInfluencedPartitions`: min(2, 2) = 2
- `dp:maxPartitionContribution`: min(1, 1) = 1

But with domain/data knowledge (if public), ColumnGroup [`year`, `month`] has metadata:
- `dp:publicPartitions`: [06, 07, 08, 09, 10, 11, 12] of 2026 and [01, 02, 03, 04, 05] of 2027.
- `dp:maxPartitionLength`: 31
- `dp:maxNumPartitions`: 12
- `dp:maxInfluencedPartitions`: 1
- `dp:maxPartitionContribution`: 1


## 3. Class diagram
```
                   ┌─────────────────┐
                   │   csvw:Table    │
                   │ ⬆ dp:DPBounded  │
                   └────────┬────────┘
                            │ dp:maxPartitionLength = dp:maxTableLength
                            │ dp:maxPartitionContribution / dp:maxInfluencedPartitions = dp:maxContributions
                            │ dp:partitionLength = dp:tableLength
                            ▼
                   ┌───────────────┐
                   │ dp:DPBounded  │
                   │ maxPartitionLength / partitionLength
                   │ maxPartitionContribution
                   │ maxInfluencedPartitions
                   └────────┬──────┘
                            │
         ┌──────────────────┴──────────────────┐
         │                                     │
 ┌───────▼─────────┐                    ┌─────▼──────────┐
 │ dp:GroupingKey  │                    │ dp:PartitionKey│
 │ (Column/Group)  │                    │ single partition│
 └───────┬─────────┘                    └────────────────┘
         │
 ┌───────▼─────────┐
 │ dp:ColumnGroup  │
 └─────────────────┘
```


## PROPERTIES
Table 
- Table Properties (some compulsory)
    - Schema
        - Columns
            - Column Properties
            - Public partitions
                - Partition identified by PartitionKey
                - Partition Properties
    - ColumnsGroup
        - property: list of columns
        - Public partitions
            - Partitions identified by components which are a list of [PartitionKey] per column
            - Partition Properties


## CONTRIBUTIONS
Table 
- Table Properties: maxContributions, maxLength
    - Schema
        - Columns
            - Column Contribution: maxInfluencedPartitions
            - Public partitions
                - Partition Properties: maxContributions, maxLength, publicLength
    - ColumnsGroup
        - ColumnGroup Contribution: maxInfluencedPartitions
        - Public partitions
            - Partition Properties: maxContributions, maxLength, publicLength

A grouping scope is determined by the grouping key of the query:
| Query type                | Governing scope           |
| ------------------------- | ------------------------- |
| No GROUP BY               | table                     |
| GROUP BY column           | that column               |
| GROUP BY multiple columns | corresponding ColumnGroup |
| Histogram over partitions | partition set             |

Implementations MUST select the nearest parent Groupable object.

For a query grouped by key G: Contribution bounds must be taken from the Groupable object representing G.
If not present, bounds are inherited from the nearest parent scope.
Order: 
```
Partition → ColumnGroup → Column → Table
```



