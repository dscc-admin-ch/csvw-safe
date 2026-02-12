# Derived columns
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

