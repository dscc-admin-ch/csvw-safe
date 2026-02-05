# CSVW Differential Privacy Extension Vocabulary

### 1.7 Full Visual Overview

In full:
```
csvw:Table
  ⊂ dp:DPBounded
  │
  ├─ dp:DP bounds
  │    ├─ dp:maxPartitionLength
  │    ├─ dp:maxPartitionContribution
  │    ├─ dp:maxInfluencedPartitions (= 1)
  │    └─ dp:partitionLength
  │
  ├─ csvw:tableSchema → csvw:TableSchema
  │      │
  │      └─ csvw:column → csvw:Column
  │             │
  │             │  csvw:Column
  │             │    ⊂ dp:GroupingKey
  │             │      ⊂ dp:DPBounded
  │             │
  │             ├─ CSVW schema metadata
  │             │    ├─ datatype
  │             │    ├─ required
  │             │    └─ minimum / maximum
  │             │
  │             ├─ dp:DP bounds
  │             │    ├─ dp:maxPartitionLength
  │             │    ├─ dp:maxPartitionContribution
  │             │    ├─ dp:maxInfluencedPartitions
  │             │    └─ dp:maxNumPartitions
  │             │
  │             └─ dp:publicPartitions   (DP-relevant)
  │                   │
  │                   └─ dp:PartitionKey
  │                         ⊂ dp:DPBounded
  │                         │
  │                         ├─ partition definition
  │                         │    ├─ dp:partitionValue
  │                         │    └─ OR dp:lowerBound / dp:upperBound
  │                         │
  │                         └─ dp:DP bounds
  │                              ├─ dp:maxPartitionLength
  │                              └─ dp:maxPartitionContribution
  │
  └─ dp:ColumnGroup
        ⊂ dp:GroupingKey
          ⊂ dp:DPBounded
            │
            ├─ dp:columns → rdf:List(csvw:Column)
            │
            ├─ dp:DP bounds
            │    ├─ dp:maxPartitionLength
            │    ├─ dp:maxPartitionContribution
            │    ├─ dp:maxInfluencedPartitions
            │    └─ dp:maxNumPartitions
            │
            └─ dp:publicPartitions     (DP-relevant)
                  │
                  └─ dp:PartitionKey
                        │
                        ├─ dp:partitionBindings   (structural)
                        │    ├─ column → literal
                        │    └─ column → interval
                        │
                        └─ dp:DP bounds
```

Diagram

- `dp:DPBounded`: The common base for anything that can carry DP limits: grouping keys and individual partitions.

- `dp:GroupingKey`: Defines a group-by universe:
    - a single column
    - or a multi-column `dp:ColumnGroup`

- `dp:PartitionKey`: Defines one publicly known partition inside a grouping key. It may optionally override or refine DP bounds.

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

