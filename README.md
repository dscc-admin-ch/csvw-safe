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
CSVW-SAFE does not describe the dataset itself but the set of datasets considered possible under the privacy model. All bounds must hold for every dataset in this set.

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


In CSVW-SAFE, there are 4 main objects on which the properties apply:
| Class                   | Purpose                        |
| ----------------------- | ------------------------------ |
| `csvw:Table`            | Dataset-level guarantees       |
| `csvw:Column`           | Column schema and grouping key |
| `csvw-safe:GroupingKey` | Multi-column grouping key      |
| `csvw-safe:Partition`   | A possible region of rows in the data domain |

- `csvw:Table`  are tables as described in `csvw`. A `csvw:Table` contains a `csvw:TableSchema` (with a list of `csvw:Columns`) and optionnaly a `csvw-safe:AdditionalInformation` (with a list of `csvw-safe:ColumnGroup` and their partitions).
- `csvw:Column` are columns as described in `csvw`.
- `csvw-safe:GroupingKey` defines a grouping space on which contribution bounds apply. A grouping key is not part of the table schema and only needs to be declared when tighter bounds than per-column bounds are known. If a GroupingKey is not declared, systems MUST assume the grouping is independent across columns and derive bounds from per-column metadata using worst-case composition.
- `csvw:Column` and `csvw-safe:ColumnGroup` can have partitions. If `csvw-safe:Partitions` is declared, it contains a list of `csvw-safe:Partition`.
- A `csvw-safe:Partition` represents a group of rows the grouping region defined by a column or a group of columns. For details on `csvw-safe:Partition`, see point 2.4 below.

CSVW-SAFE structural and contribution properties apply on these four main classes. 

![Overview](images/csvw-safe_structure.png)

The properties belong to two categories:
| Aspect                 | Describes                                     | Name               | 
| ---------------------- | --------------------------------------------- | ------------------ |
| Public invariant facts | true structural facts about the data universe | `csvw-safe:public.` |
| Model assumptions      | Used to compute worst-case DP sensitivity     | `csvw-safe:bounds.`  |
| Synthetic data modelling |  Synthetic or dummy closer to original      | `csvw-safe:synth.`  |


`csvw-safe:public.` statements hold for every dataset in the adjacency relation. They may be released without consuming privacy budget and therefore must hold for all neighbouring datasets.
`csvw-safe:bounds.` must hold for all datasets consistent with the declared public constraints, regardless of observed data.

---

## 2. CSVW-SAFE Main Extensions (better title?)

A privacy unit defines dataset adjacency. Two datasets are neighbours if and only if all rows associated with one value of the privacy unit are added or removed.

CSVW-SAFE assumes bounded user-level differential privacy where neighboring datasets differ by all rows associated with one or more privacy units. 

A privacy unit is an identifier representing an individual or entity whose data must be protected (e.g. `patient_id`, `user_id`, `hospital_id`).

Contribution bounds describe how much influence one privacy unit can have on the output.


### 2.1 All Levels

We define 6 new terms that can be used to infer DP bounds. 

| Name                                     | Table    | Partition | Column         | GroupingKey    |
|------------------------------------------|---------:|----------:|---------------:|----------------|
| `csvw-safe:bounds.maxContributions`       | Yes (C)  | Yes       | No             | No             |
| `csvw-safe:bounds.maxGroupsPerUnit`       | 1        | 1         | Yes            | Yes            |
| `csvw-safe:bounds.maxLength`              | Yes (C)  | Yes       | No             | No             |
| `csvw-safe:bounds.maxNumPartitions`       | No       | No        | Yes            | Yes            |
| `csvw-safe:public.length`                | Yes      | Yes       | No             | No             |
| `csvw-safe:public.partitions`            | No       | No        | Yes            | Yes            |

(C): means compulsory to apply DP. The rest is optional and will avoid wasting budget on public information and avoir overstimating sensitivity.

`csvw-safe:bounds.maxContributions` ($l_\infty$) maximum number of rows contributed by a single privacy unit to any one grouping region.
- At the table level, it is the maximum number of rows a privacy unit may contribute to the entire dataset. This bound governs sensitivity of queries without grouping. It is compulsory to apply DP.
- At the partition level, it is the maximum number of rows in the partition which concern the privacy unit.

`csvw-safe:bounds.maxGroupsPerUnit` ($l_0$) is the maximum number of groups produced by a grouping operation on this key in which a single privacy unit may appear. The grouping key is the `csvw:Column` or `csvw-safe:GroupingKey` on which the property is declared.
- At the table level, it does not make sense and is 1.
- At the partition level, it does not make sense and is 1.
- At the column level, it is the number of partitions of the column (after a groupby) that can be affected by an individual.
- At the multiple column level, it is the number of partitions of the group of columns (after a groupby) that can be affected by an individual. In the worst case, the product of the number of partitions of all individual columns.

**Note**:These parameters allow systems to determine the maximum number of rows that may change if one privacy unit is added or removed. 
The total number of rows a privacy unit may influence $l_1 = l_0 \cdot l_\infty$ is not defined as a new word as it depends on the query and $l_\infty$ and $l_0$.

`csvw-safe:bounds.maxLength` is the maximum theoretical number of rows. Is also enables to compute additional noise requirements in case of overflow when doing some operations. See reference: [Casacuberta et al., 2022](https://dl.acm.org/doi/pdf/10.1145/3548606.3560708).
- At the table level, it is the maximum theoretical number of rows in the table. It is compulsory to apply DP.
- At the partition level, it is the maximum theoretical number of rows in the partition.

`csvw-safe:public.length` is the exact number of rows if it is known (if it is public information). If given, it allows exact denominator in sensitivity computation. For instance, for a mean, all the budget is spent for the sum and then divided by publicLength instead of splitting the budget in half for a count and a sum.
- At the table level, it is the number of rows in the table. 
- At the partition level, it is the number of rows in the partition.
- It does not make sense at `csvw-safe:Column` and `csvw-safe:GroupingKey` level as it is the same as at `csvw:Table` level.

`csvw-safe:bounds.maxNumPartitions` refers to the maximum number of non-empty groups that may appear in a query result, not the size of the value domain. (TODO)
- At the column level, it is the number of different categories in the column.
- At the group of columns level, it is the number of different partitions that can be produced by grouping multiple columns (cartesian product of the partitions of each column in the simplest case).
If `public.partitions` is declared and `exhaustivePartitions=true`, then maxNumPartitions equals the number of declared partitions. Otherwise, `maxNumPartitions` must be explicitly declared. TODO: is it csvw-safe:synth ???

`csvw-safe:public.partitions` is the list of known public partitions in a column or group of column. They are made of `csvw-safe:Partition` (see section 2.3 on partitions level structural properties).
- At the column level, it is the list of public `csvw-safe:Partition` of a given column.
- At the group of columns level, it is the list of public `csvw-safe:Partition` produced by grouping multiple columns.
This enables to avoid spending budget (delta) to release partitions name if already public.

Along with `csvw-safe:public.partitions`, the term `csvw-safe:public.exhaustivePartitions` is used. If all partitions are public and given in `csvw-safe:public.partitions`, then it is True, otherwise, it is False. `csvw-safe:public.exhaustivePartitions` applies to `csvw-safe:Column` and `csvw-safe:GroupingKey` objects. Null values form an implicit partition unless prohibited by `required=true`.


### 2.2 Contribution with respect to an privacy unit
Differential privacy guarantees are defined relative to a privacy unit.

A privacy unit identifies the entity whose participation defines dataset adjacency (e.g. `patient_id`, `user_id`, `hospital_id`).
Contribution bounds describe how much influence one privacy unit may have on query results.

Two parameters are used:
| Bound                                | Symbol     | Meaning                                                                |
| ------------------------------------ | ---------- | ---------------------------------------------------------------------- |
| `csvw-safe:bounds.maxContributions`  | $l_\infty$ | Maximum rows contributed by a privacy unit inside one partition region |
| `csvw-safe:bounds.maxGroupsPerUnit`  | $l_0$      | Maximum number of partition regions a privacy unit may affect          |

**Privacy contribution object**: Contribution bounds are defined using `csvw-safe:Contribution`.

A contribution attaches bounds to a specific privacy unit.

| Property                            | Meaning                                         |
| ----------------------------------- | ----------------------------------------------- |
| `csvw-safe:public.privacyUnit`      | identifier column defining the protected entity |
| `csvw-safe:bounds.maxContributions` | per-region row bound                            |
| `csvw-safe:bounds.maxGroupsPerUnit` | number of affected regions                      |

Sensitivity must be computed with respect to one declared privacy unit at a time.
A mechanism calibrated for one privacy unit does not automatically guarantee privacy for another unless explicitly composed.

#### 2.2.1 Single Privacy Unit: 
If exactly one privacy unit exists, it may be declared at table level:
```
"csvw-safe:public.privacyUnit": "patient_id"
```
In this case, contribution bounds may be written directly without wrapping in `csvw-safe:Contribution`.

Example at column level:
```
{
  "@type": "csvw:Column",
  "name": "disease",
  "csvw-safe:bounds.maxGroupsPerUnit": 10
}
```

Example at partition level:
```
"csvw-safe:public.partitions":[
  {
    "@type":"csvw-safe:Partition",
    "csvw-safe:predicate": { "partitionValue": "Adelie" },
    "csvw-safe:bounds.maxContributions": 1
  },
  {
    "@type":"csvw-safe:Partition",
    "csvw-safe:predicate": { "partitionValue": "Chinstrap" },
    "csvw-safe:bounds.maxContributions": 1
  }
]
```


#### 2.2.2 Multiple Privacy Unit: 
If multiple privacy units exist, bounds must be defined separately for each unit.

Example at column level:
```
{
  "@type": "csvw:Column",
  "name": "disease",
  "csvw-safe:contributions": [
    {
      "@type": "csvw-safe:Contribution",
      "csvw-safe:public.privacyUnit": "patient_id",
      "csvw-safe:bounds.maxGroupsPerUnit": 10
    },
    {
      "@type": "csvw-safe:Contribution",
      "csvw-safe:public.privacyUnit": "hospital_id",
      "csvw-safe:bounds.maxGroupsPerUnit": 2
    }
  ]
}
```

Example at partition level:
```
"csvw-safe:public.partitions":[
  {
    "@type":"csvw-safe:Partition",
    "csvw-safe:predicate": { "partitionValue":"Adelie" },
    "csvw-safe:contributions":[
      {
        "@type": "csvw-safe:Contribution",
        "csvw-safe:public.privacyUnit": "patient_id",
        "csvw-safe:bounds.maxContributions": 1
      },
      {
        "@type": "csvw-safe:Contribution",
        "csvw-safe:public.privacyUnit": "hospital_id",
        "csvw-safe:bounds.maxContributions": 2
      }
    ]
  }
]
```

#### 2.2.3 Adjacency definition (TODO: not sure, is it really relevant)
The dataset must declare the neighboring relation used for `csvw-safe:bounds.*`: `csvw-safe:adjacencyDefinition` with default `addRemoveUnit`.
The values can be:
| Value           | Meaning                                 |
| --------------- | --------------------------------------- |
| `addRemoveUnit` | add/remove all rows of one privacy unit |
| `replaceUnit`   | replace all rows of one privacy unit    |
| `addRemoveRow`  | row-level adjacency                     |
| `custom`        | formally specified externally           |

#### 2.2.4 Adjacency definition (TODO: not sure, is it really relevant)
If more than one privacy unit exists, the dataset must specify: `csvw-safe:privacyModel`.
The values can be:
| Value          | Meaning                                     |
| -------------- | ------------------------------------------- |
| `independent`  | guarantees provided separately per unit     |
| `hierarchical` | units nested (e.g. patient inside hospital) |
| `joint`        | adjacency defined on combined unit          |

#### 2.2.5 Structural hierarchy for contribution bounds
```
adjacencyDefinition
privacyModel

Table
 ├─ bounds.maxContributions
 ├─ bounds.maxLength
 │
 ├─ Columns
 │   ├─ bounds.maxGroupsPerUnit
 │   └─ public.partitions
 │        └─ Partition
 │             ├─ predicate
 │             ├─ public.*
 │             └─ bounds.maxContributions / bounds.maxLength
 │
 └─ GroupingKey
     ├─ bounds.maxGroupsPerUnit
     └─ public.partitions
          └─ Partition
               ├─ predicate (components)
               └─ bounds.*
```

### 2.3 Minimum Metadata for Worst-Case Sensitivity
This section defines the minimum metadata required for a system to compute sound worst-case sensitivity bounds for differentially private mechanisms.

A dataset is considered DP-calibratable only if all mandatory bounds required by the chosen adjacency definition are present.

Sensitivity is computed relative to:
- the declared `csvw-safe:adjacencyDefinition`
- the declared privacy unit(s) `public.privacyUnit`
- declared contribution bounds
- range of value for some queries on continuous values

For each declared privacy unit, the table must define:
| Property                            | Purpose                                                                                        |
| ----------------------------------- | ---------------------------------------------------------------------------------------------- |
| `csvw-safe:bounds.maxContributions` | Maximum number of rows contributed by one privacy unit to the entire table (global (l_\infty)) |
| `csvw-safe:bounds.maxLength`        | Maximum possible number of rows in the dataset                                                 |

If multiple privacy units exist, the bounds apply independently per unit according to `csvw-safe:privacyModel`.

For any column used in a numeric aggregation (SUM, MEAN, VAR, STDDEV, etc.), the column must declare a closed value domain:
| Property  | Meaning                |
| --------- | ---------------------- |
| `minimum` | smallest allowed value |
| `maximum` | largest allowed value  |

These bounds define the per-row contribution range and are necessary to compute aggregation sensitivity.
A system must refuse DP calibration for a numeric aggregation if these bounds are missing.


---


## 3. Structural Modeling Extensions

Structural metadata supports:

- Dummy dataset generation (generate a dataset that has the same schema and structure as the original dataset) for functional programming for instance.
- Public schema discovery (can already answer some questions without requiring private data access).

All standard CSVW column properties (`datatype`, `format`, `minimum`, `maximum`, `required`, `default`) are re-used as is.
In particular, for continuous columns, `minimum` and `maximum` are compusory to apply DP.

### 3.1 Column-Level Structural Properties

For structural purposes, other fields exist on the `csvw:Column`:

| Term                                  | Type                                  | Meaning                                             |
| ------------------------------------- | ------------------------------------- | --------------------------------------------------- |
| `csvw-safe:public.privacyId`          | boolean                               | True if column identifies individuals/units         |
| `csvw-safe:synth.nullableProportion`  | decimal (0–1)                         | Approximate fraction of null values                 |
| `csvw-safe:synth.dependsOn`           | column reference                      | Declares dependency on another column               |
| `csvw-safe:synth.how`                 | enum (`bigger`, `smaller`, `mapping`) | Type of dependency                                  |
| `csvw-safe:synth.mapping`             | object                                | Required if `how = mapping`                         |

**Dependency Rules**
- `dependsOn` and `how` MUST be provided together.
- If `how = mapping`, then `mapping` MUST be provided.

**Notes**
- `csvw-safe:synth.nullableProportion` improves modeling beyond csvw:required.
- maxNumPartitions describes grouping universe size but does not affect sensitivity unless combined with DP bounds.
- multiple columns may have `csvw-safe:public.privacyId=true`. In these cases, DP contributions (section 3) must be provided per privacy unit.

### 3.2 GroupingKey-Level Structural Properties
`csvw-safe:GroupingKey` represents a grouping key composed of multiple columns

| Property            | Meaning                             |
| ------------------- | ----------------------------------- |
| `csvw-safe:public.columns` | Ordered list of constituent columns |

If a `csvw-safe:GroupingKey` is declared, all referenced columns must exist in the table schema.

A `GroupingKey` defines a joint grouping space. It does not automatically enumerate all combinations; explicit partitions may optionally restrict this space (see Partitions-Level below).

### 3.3 Partition-Level Structural Properties

#### Concept (TODO: how to explain better?)
A partition describes a publicly defined region of rows determined solely by public attributes.

A partition describes a region in the grouping universe.
A group is the subset of rows in a specific dataset instance belonging to that region.
A partition is a region of the value domain.
A group is the result of applying a grouping operation to a dataset.
Each group corresponds to exactly one declared partition, but a partition may correspond to zero groups if no rows fall inside it.

A grouping query (e.g. GROUP BY) produces groups that correspond to these regions. Differential privacy bounds are defined with respect to these grouping regions, not to the physical rows present in the dataset.

A partition is composed of:
- Predicate — how rows are selected
- Partition properties — public facts and/or safety bounds about that region

A row belongs to a partition iff it satisfies its predicate.

Partitions may be declared for:
- a `csvw:Column`
- a `csvw-safe:GroupingKey`

| Term                | Meaning                                                  |
| ------------------- | -------------------------------------------------------- |
| Partition           | A region of rows                                         |
| Predicate           | Logical condition defining membership                    |
| Component predicate | Predicate for one column within a multi-column partition |

A partition is the conjunction of one or more predicates.


**Public Partitions**: `csvw-safe:public.partitions` declares known regions of the dataset domain.
These describe the grouping universe, not the observed data.
If all possible regions are declared, `csvw-safe:public.exhaustivePartitions = true`.


**Partition**: A partition contains:
| Field                 | Meaning                |
| --------------------- | ---------------------- |
| `csvw-safe:predicate` | Membership condition   |
| `csvw-safe:public.*`  | Public invariant facts |
| `csvw-safe:bounds.*`  | DP safety bounds       |


**Predicate**: A predicate defines row membership. The fields are
| Property         | Meaning                               |
| ---------------- | ------------------------------------- |
| `partitionValue` | categorical equality                  |
| `lowerBound`     | numeric lower bound                   |
| `upperBound`     | numeric upper bound                   |
| `lowerInclusive` | default true                          |
| `upperInclusive` | default false                         |
| `components`     | map column → predicate (multi-column) |



For `csvw:Column` with categorical data, the partition can be identified by `csvw-safe:partitionValue`.
```
{
  "name": "sex",
  "datatype": "string",
  "csvw-safe:public.partitions": [
    {
      "@type": "csvw-safe:Partition",
      "csvw-safe:predicate": {
        "partitionValue": "MALE"
      },
      "csvw-safe:bounds.maxContributions": 1
    },
    {
      "@type": "csvw-safe:Partition",
      "csvw-safe:predicate": {
        "partitionValue": "FEMALE"
      },
      "csvw-safe:bounds.maxLength": 50
    }
  ]
}
```
For `csvw:Column` with continuous data, the partition can be identified by `csvw-safe:public.lowerBound`, `csvw-safe:public.upperBound`, `csvw-safe:public.lowerInclusive` and `csvw-safe:public.upperInclusive` fields.
```
{
  "name": "flipper_length_mm",
  "datatype": "double",
  "minimum": 150.0,
  "maximum": 250.0,
  "csvw-safe:public.partitions": [
    {
      "@type": "csvw-safe:Partition",
      "csvw-safe:predicate": {
        "lowerBound": 150.0,
        "upperBound": 200.0
      }
    },
    {
      "@type": "csvw-safe:Partition",
      "csvw-safe:predicate": {
        "lowerBound": 200.0,
        "upperBound": 250.0
      }
    }
  ]
}
```
For `csvw:GroupingKey` with categorical data, the partition can be identified by `csvw-safe:public.components` and then a partition per column.
```
{
  "@type": "csvw-safe:GroupingKey",
  "csvw-safe:columns": ["sex", "island"],
  "csvw-safe:public.partitions": [
    {
      "@type": "csvw-safe:Partition",
      "csvw-safe:predicate": {
        "components": {
          "sex": { "partitionValue": "MALE" },
          "island": { "partitionValue": "Torgersen" }
        }
      }
    }
  ]
}
```
Similarly for a `csvw:GroupingKey` with categorical and continuous data, the partition can be identified by `csvw-safe:public.components` and then a partition per column.
```
{
  "@type": "csvw-safe:GroupingKey",
  "csvw-safe:columns": ["sex", "flipper_length_mm"],
  "csvw-safe:public.partitions": [
    {
      "@type": "csvw-safe:Partition",
      "csvw-safe:predicate": {
        "components": {
          "sex": { "partitionValue": "MALE" },
          "flipper_length_mm": {
            "lowerBound": 150.0,
            "upperBound": 200.0
          }
        }
      }
    }
  ]
}
```

Structural hierarchy
```
Table
 ├─ Columns
 │   └─ public.partitions
 │       ├─ predicate
 │       └─ partition properties
 │
 └─ GroupingKey
     └─ public.partitions
         ├─ component predicates
         └─ partition properties
```


---



## 5. CSVW-SAFE Framework

| File                          | Purpose                             |
| ----------------------------- | ----------------------------------- |
| `README.md`                   | Description, Motivation             |
| `csvw-safe-constraints.md`    | Constraints explanations            |
| `csvw-safe-vocab.ttl`         | Vocabulary definition (OWL + RDFS)  |
| `csvw-safe-context.jsonld`    | JSON-LD context                     |
| `csvw-safe-constraints.ttl`   | SHACL validation rules              |
| `penguin_metadata.json`       | Example metadata                    |
| `dp_libraries.md`             | Mapping to DP libraries             |
| `validate_metadata.py`        | Metadata validator                  |
| `make_metadata_from_data.py`  | Infer baseline CSVW metadata        |
| `make_dummy_from_metadata.py` | Dummy data generator                |
| `assert_same_structure.py`    | Verify functional programming valid on dummy will be valid on real data |

This library provides Python utilities for generating, validating, and testing CSVW-SAFE metadata and associated dummy datasets for differential privacy (DP) development and safe data modeling workflows.

It includes four main scripts:

1. make_metadata_from_data.py
2. make_dummy_from_metadata.py
3. validate_metadata.py
4. assert_same_structure.py

This is available in a pip library `csvw-safe-lib` described in [the README.md of `csvw-safe-lib`](https://github.com/dscc-admin-ch/csvw-safe/blob/main/csvw-safe-library/README.md).

![Overview](images/utils_scripts.png)


