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


### 1.1 Main classes

CSVW-SAFE uses four core objects on which structural and privacy properties apply:

| Class                   | Purpose                                                 |
| ----------------------- | ------------------------------------------------------- |
| `csvw:Table`            | Dataset-level guarantees and global contribution bounds |
| `csvw:Column`           | Column schema and single-column grouping space          |
| `csvw-safe:GroupingKey` | Multi-column grouping space                             |
| `csvw-safe:Partition`   | A region of the value domain (not the rows themselves)  |

- `csvw:Table` are tables as described in `csvw`. A `csvw:Table` contains a `csvw:TableSchema` (with a list of `csvw:Columns`) and optionally a `csvw-safe:AdditionalInformation` (with a list of `csvw-safe:ColumnGroup` and their partitions).

- `csvw:Column` are columns as described in `csvw`. It also defines a single column grouping space.

- `csvw-safe:GroupingKey` defines a multi-column grouping space (not part of schema).It represents the structure of a potential GROUP BY operation. There are two possible types:
- Single-column grouping → defined directly on a csvw:Column
- Multi-column grouping → defined using csvw-safe:GroupingKey.
If no GroupingKey is declared, systems must assume independence across columns and bounds are derived using worst-case composition (sensitivity may be overestimated).
As a result `csvw:Column` and `csvw-safe:GroupingKey` may declare public partitions. Each declared partition is a `csvw-safe:Partition`.

- A `csvw-safe:Partition` is a publicly defined region of the value domain. It is a structural element defined from public attributes and independent of whether rows exist in that region. For details on `csvw-safe:Partition`, see point 2.4 below.
    - A partition is a region of the value domain defined only from public information. A partition may exist even if no rows belong to it.
    - A group is the set of rows from a specific dataset instance that fall inside that region. A group only exists when rows actually fall into that partition.

#### Example

Example with a penguin dataset example.
We have a `csvw:Table` with 4 rows and 3 `csvw:Columns`:
| penguin_id | species   | island    | flipper_length_mm |
| ---------- | --------- | --------- | ----------------- |
| 1          | Adelie    | Torgersen | 180               |
| 2          | Adelie    | Biscoe    | 195               |
| 3          | Chinstrap | Dream     | 200               |
| 4          | Gentoo    | Biscoe    | 210               |

Some columns have public partitions: `csvw-safe:Partitions`
- `csvw:Column` species → categorical `csvw-safe:Partition` by species value: Adelie, Chinstrap, Gentoo.
- `csvw:Column` island → categorical `csvw-safe:Partition` by island value: Torgersen, Biscoe, Dream.
- `csvw:Column` flipper_length_mm → numeric `csvw-safe:Partition`: [150–200], [200–250] (for instance).

with the JSON
```
{
  "@type": "csvw:Table",
  "name": "penguins",
  "csvw:tableSchema": {
    "columns": [
      {
        "@type": "csvw:Column",
        "name": "penguin_id",
        "datatype": "integer"
      },
      {
        "@type": "csvw:Column",
        "name": "species",
        "datatype": "string",
        "csvw-safe:public.partitions": [
          { "@type": "csvw-safe:Partition", "csvw-safe:predicate": { "partitionValue": "Adelie" }},
          { "@type": "csvw-safe:Partition", "csvw-safe:predicate": { "partitionValue": "Chinstrap" }},
          { "@type": "csvw-safe:Partition", "csvw-safe:predicate": { "partitionValue": "Gentoo" }}
        ]
      },
      {
        "@type": "csvw:Column",
        "name": "island",
        "datatype": "string",
        "csvw-safe:public.partitions": [
          { "@type": "csvw-safe:Partition", "csvw-safe:predicate": { "partitionValue": "Torgersen" }},
          { "@type": "csvw-safe:Partition", "csvw-safe:predicate": { "partitionValue": "Biscoe" }},
          { "@type": "csvw-safe:Partition", "csvw-safe:predicate": { "partitionValue": "Dream" }}
        ]
      },
      {
        "@type": "csvw:Column",
        "name": "flipper_length_mm",
        "datatype": "double",
        "minimum": 150,
        "maximum": 250,
        "csvw-safe:public.partitions": [
          { "@type": "csvw-safe:Partition", "csvw-safe:predicate": { "lowerBound": 150, "upperBound": 200 }},
          { "@type": "csvw-safe:Partition", "csvw-safe:predicate": { "lowerBound": 200, "upperBound": 250 }}
        ]
      }
    ]
  }
}
```
A `csvw-safe:GroupingKey` would by for instance based on columns ["species", "island"]. And the resulting partitions would be
- `csvw-safe:GroupingKey` ["species", "island"] → categorical `csvw-safe:Partition` by species and island values: (Adelie, Torgerson), (Adelie, Biscoe), (Chinstrap, Dream), (Gentoo, Biscoe).
- `csvw-safe:GroupingKey` ["species", "flipper_length_mm"] → `csvw-safe:Partition` by species and island values: (Adelie, [150–200]), (Chinstrap, [150–200]), (Gentoo, [200–250]).

Adding to the JSON
```
{
  "@type": "csvw:Table",
  "name": "penguins",
  "csvw:tableSchema": {...},
  "csvw-safe:additionalInformation": [
    {
      "@type": "csvw-safe:GroupingKey",
      "csvw-safe:columns": ["species", "island"],
      "csvw-safe:public.partitions": [
        {
          "@type": "csvw-safe:Partition",
          "csvw-safe:predicate": {
            "components": {
              "species": { "partitionValue": "Adelie" },
              "island": { "partitionValue": "Torgersen" }
            }
          }
        },
        {
          "@type": "csvw-safe:Partition",
          "csvw-safe:predicate": {
            "components": {
              "species": { "partitionValue": "Adelie" },
              "island": { "partitionValue": "Biscoe" }
            }
          }
        }
      ]
    }
  ]
}
```

### 1.2 Type of Properties
![Overview](images/csvw-safe_structure.png)

CSVW-SAFE properties belong to three categories:
| Aspect                                 | Describes                                             | Namespace prefix    |
| -------------------------------------- | ----------------------------------------------------- | ------------------- |
| **Public invariant facts**             | True structural facts about the data universe         | `csvw-safe:public.` |
| **Model assumptions (privacy bounds)** | Worst-case assumptions used to compute DP sensitivity | `csvw-safe:bounds.` |
| **Synthetic modeling hints**           | Information for generating realistic dummy data       | `csvw-safe:synth.`  |

These properties should only be in the metadata if their release does not consume privacy budget.
The should not depend on the observed dataset instance (not specific empirical observations) and should hold for all neighbouring datasets.

**Public invariant facts**: `csvw-safe:public.`
These describe facts that are true for every dataset in the adjacency relation.
If a statement is declared as `csvw-safe:public.`, it is assumed to be universally valid and safe to disclose.

**Model assumptions**: `csvw-safe:bounds.`
A privacy unit identifies the entity whose participation defines dataset adjacency. Two datasets are neighbours if and only if all rows associated with one privacy unit are added or removed.
`csvw-safe:bounds.` define worst-case contribution bounds required for DP calibration (maximum influence one privacy unit may have on a query result).
They must hold for all datasets consistent with the declared public constraints and are guarantees about the possible universe of neighbouring datasets.

**Synthetic modeling hints**: `csvw-safe:synth.`
These properties are optional and serve dummy data generation.

They improve the realism of synthetic datasetsa and dummy dataset but should not affect DP guarantees. They may be approximate proportions.

---

## 2. CSVW-SAFE Main Extensions (better title?)

A privacy unit defines dataset adjacency. Two datasets are neighbours if and only if all rows associated with one value of the privacy unit are added or removed.

CSVW-SAFE assumes bounded user-level differential privacy where neighboring datasets differ by all rows associated with one or more privacy units. 

A privacy unit is an identifier representing an individual or entity whose data must be protected (e.g. `patient_id`, `user_id`, `hospital_id`).

Contribution bounds describe how much influence one privacy unit can have on the output.


### 2.1 All Levels

We define 6 new terms that can be used to infer DP bounds. 

| Term                      | TL;DR                                                                  |
| ------------------------- | ---------------------------------------------------------------------- |
| `bounds.maxContributions` | Max rows a privacy unit can contribute in a region (l∞)                |
| `bounds.maxGroupsPerUnit` | Max groups / partitions a privacy unit can appear in (l0)              |
| `bounds.maxLength`        | Max rows in table / partition (theoretical upper bound)                |
| `bounds.maxNumPartitions` | Max number of non-empty output partitions for a column or grouping key |
| `public.length`           | Exact number of rows if public                                         |
| `public.partitions`       | List of known partitions (publicly known regions)                      |

They can be properties of different classes:

| Name                                     | Table           | Partition | Column         | GroupingKey    |
|------------------------------------------|----------------:|----------:|---------------:|----------------|
| `csvw-safe:bounds.maxContributions`      | Yes (required)  | Yes       | No             | No             |
| `csvw-safe:bounds.maxGroupsPerUnit`      | 1               | 1         | Yes            | Yes            |
| `csvw-safe:bounds.maxLength`             | Yes (required)  | Yes       | No             | No             |
| `csvw-safe:bounds.maxNumPartitions`      | No              | No        | Yes            | Yes            |
| `csvw-safe:public.length`                | Yes             | Yes       | No             | No             |
| `csvw-safe:public.partitions`            | No              | No        | Yes            | Yes            |

Required values are mandatory for DP calibration.
Others improve tightness and avoid unnecessary noise.

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

`csvw-safe:bounds.maxLength` is the maximum theoretical number of rows. It also enables to compute additional noise requirements in case of overflow when doing some operations. See reference: [Casacuberta et al., 2022](https://dl.acm.org/doi/pdf/10.1145/3548606.3560708).
- At the table level, it is the maximum theoretical number of rows in the table. It is compulsory to apply DP.
- At the partition level, it is the maximum theoretical number of rows in the partition.

`csvw-safe:public.length` is the exact number of rows if it is known (if it is public information). If given, it allows exact denominator in sensitivity computation. For instance, for a mean, all the budget is spent for the sum and then divided by publicLength instead of splitting the budget in half for a count and a sum. length is invariant across neighbors.
- At the table level, it is the number of rows in the table. 
- At the partition level, it is the number of rows in the partition.
- It does not make sense at `csvw-safe:Column` and `csvw-safe:GroupingKey` level as it is the same as at `csvw:Table` level.

`csvw-safe:bounds.maxNumPartitions` refers to the maximum number of non-empty groups that may appear in a query result, not the size of the value domain.
- At the column level, it is the number of different categories in the column. For instance, a column with 3 categories has `maxNumPartitions=3`.
- At the group of columns level, it is the number of different partitions that can be produced by grouping multiple columns (cartesian product of the partitions of each column in the simplest case).
If `public.partitions` is declared and `exhaustivePartitions=true`, then maxNumPartitions equals the number of declared partitions. Otherwise, `maxNumPartitions` must be explicitly declared.

`csvw-safe:public.partitions` is the list of known public partitions in a column or group of column. They are made of `csvw-safe:Partition` (see section 2.3 on partitions level structural properties).
- At the column level, it is the list of public `csvw-safe:Partition` of a given column.
- At the group of columns level, it is the list of public `csvw-safe:Partition` produced by grouping multiple columns.
This enables to avoid spending budget (delta) to release partitions name if already public.

Along with `csvw-safe:public.partitions`, the term `csvw-safe:public.exhaustivePartitions` is used. If all partitions are public and given in `csvw-safe:public.partitions`, then it is True, otherwise, it is False. `csvw-safe:public.exhaustivePartitions` applies to `csvw-safe:Column` and `csvw-safe:GroupingKey` objects. Null values form an implicit partition unless prohibited by `required=true`.

This is an example when there is only one known privacy unit: penguin_id.

```
{
  "@type": "csvw:Table",
  "name": "penguins",

  "csvw-safe:public.privacyUnit": "penguin_id",

  "csvw-safe:bounds.maxContributions": 3,
  "csvw-safe:bounds.maxLength": 1000,
  "csvw-safe:public.length": 342,

  "csvw:tableSchema": {
    "columns": [

      {
        "@type": "csvw:Column",
        "name": "species",
        "datatype": "string",

        "csvw-safe:bounds.maxGroupsPerUnit": 2,
        "csvw-safe:bounds.maxNumPartitions": 3,
        "csvw-safe:public.exhaustivePartitions": true,

        "csvw-safe:public.partitions": [
          {
            "@type": "csvw-safe:Partition",
            "csvw-safe:predicate": {"partitionValue": "Adelie"},
            "csvw-safe:bounds.maxContributions": 1,
            "csvw-safe:bounds.maxLength": 200
          },
          {
            "@type": "csvw-safe:Partition",
            "csvw-safe:predicate": {"partitionValue": "Gentoo"},
            "csvw-safe:bounds.maxContributions": 1,
            "csvw-safe:bounds.maxLength": 200
          },
          {
            "@type": "csvw-safe:Partition",
            "csvw-safe:predicate": {"partitionValue": "Chinstrap"},
            "csvw-safe:bounds.maxContributions": 1,
            "csvw-safe:bounds.maxLength": 200
          }
        ]
      },

      {
        "@type": "csvw:Column",
        "name": "flipper_length_mm",
        "datatype": "double",
        "minimum": 150,
        "maximum": 250,

        "csvw-safe:bounds.maxGroupsPerUnit": 2,
        "csvw-safe:bounds.maxNumPartitions": 2,

        "csvw-safe:public.partitions": [
          {
            "@type": "csvw-safe:Partition",
            "csvw-safe:predicate": {"lowerBound":150,"upperBound":200},
            "csvw-safe:bounds.maxContributions":1
          },
          {
            "@type": "csvw-safe:Partition",
            "csvw-safe:predicate": {"lowerBound":200,"upperBound":250},
            "csvw-safe:bounds.maxContributions":1
          }
        ]
      }
    ]
  },

  "csvw-safe:GroupingKeys":[
    {
      "@type":"csvw-safe:GroupingKey",
      "csvw-safe:columns":["species","flipper_length_mm"],

      "csvw-safe:bounds.maxGroupsPerUnit":3,
      "csvw-safe:bounds.maxNumPartitions":6,

      "csvw-safe:public.partitions":[
        {
          "@type":"csvw-safe:Partition",
          "csvw-safe:predicate":{
            "components":{
              "species":{"partitionValue":"Adelie"},
              "flipper_length_mm":{"lowerBound":150,"upperBound":200}
            }
          },
          "csvw-safe:bounds.maxContributions":1
        }
      ]
    }
  ]
}
```


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

- A contribution attaches bounds to a specific privacy unit:  `csvw-safe:public.privacyUnit`, which identifies the column defining the protected entity.
- Sensitivity must be computed with respect to one declared privacy unit at a time.
- A mechanism calibrated for one privacy unit does not automatically guarantee privacy for another unless explicitly composed.

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


If more than one privacy unit exists, the dataset must specify: `csvw-safe:privacyModel`.
The values can be:
| Value          | Meaning                                     |
| -------------- | ------------------------------------------- |
| `independent`  | guarantees provided separately per unit     |
| `hierarchical` | units nested (e.g. patient inside hospital) |
| `joint`        | adjacency defined on combined unit          |


#### 2.2.3 Structural hierarchy for contribution bounds
```
privacyModel

Table
 ├─ bounds.maxContributions
 ├─ bounds.maxLength
 │
 ├─ Columns
 |   ├─ datatype, min/max, required
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
               ├─ public.*
               └─ bounds.maxContributions / bounds.maxLength
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


### 2.4 Other description

#### More on `maxNumPartitions`

- maxNumPartitions does not limit the per-unit contribution.
- It limits the maximum number of output groups (non-empty partitions) a query could produce.
- This is relevant for noise allocation in certain DP mechanisms (like hierarchical counting, per-partition noise, or when bounding the maximum size of vector-valued queries).
- It does not reduce sensitivity; it’s more about knowing the potential output size so the DP library can avoid under- or over-allocating noise.

| Concept              | Meaning                         | Example          |
| -------------------- | ------------------------------- | ---------------- |
| domain size          | all possible values             | age \in [0, 120] |
| partitions           | possible output groups          | {0, 1, ..., 120} |
| non-empty partitions | groups that can actually appear | {18, 19, 20, 21} |

DP error depends on non-empty partitions. In the example: maximum vector length of 4.

#### How DP library would use CSVW-SAFE parameters

| Axis                      | Parameter                              | Effect in DP computation                                                                    |
| ------------------------- | -------------------------------------- | ------------------------------------------------------------------------------------------- |
| **Per-unit impact**       | `maxContributions`, `maxGroupsPerUnit` | Compute Δf for a single privacy unit under the chosen adjacency metric.                     |
| **Per-query output size** | `maxNumPartitions`                     | Defines the number of entries the noise must cover. Useful for vector/group queries.        |

**Workflow**:

1. DP library reads metadata (l_\infty, l_0, maxNumPartitions) and the adjacency metric.
2. Computes worst-case sensitivity: $sensitivity=f(l_0, l_\infty, adjacency)$.
3. Computes noise scale using sensitivity and output size: $noise scale ~ g(sensitivity, maxNumPartitions)$.

The adjacency definition does not change the metadata, it changes how the library interprets $l_\infty$ and $l_0$ to compute the sensitivity. Thus, we do not define it in the metadata.

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

#### Concept

A partition is a publicly defined region of the value domain determined solely by public attributes. It describes a region in the grouping universe, (not a subset of observed rows).

It is differnt to a group:
- Partition → region of possible values (structural, dataset-independent). A partition may correspond to zero groups if no rows fall into it.
- Group → rows of a particular dataset that satisfy the partition predicate. Each group corresponds to exactly one declared partition.

Differential privacy contribution bounds are defined with respect to partitions (grouping regions), not to the physical rows currently present.

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


**Predicate**: A predicate defines row membership. 
The predicate can show how rows are selcted for categorical and for numeric columns

The fields are:
| Property         | Meaning                               | Type of Columns |
| ---------------- | ------------------------------------- |---------------- |
| `partitionValue` | categorical equality                  | categorical     |
| `lowerBound`     | numeric lower bound                   | numeric         |
| `upperBound`     | numeric upper bound                   | numeric         |
| `lowerInclusive` | default true                          | numeric         |
| `upperInclusive` | default false                         | numeric         |
| `components`     | map column → predicate (multi-column) | GroupingKey     |



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

[`csvw-safe-constraints.md`](https://github.com/dscc-admin-ch/csvw-safe/blob/main/csvw-safe-constraints.md) describes constraints on metadata, ensure that they are valid and not worst than worst case bounds. [`csvw-safe-constraints.ttl`](https://github.com/dscc-admin-ch/csvw-safe/blob/main/csvw-safe-constraints.ttl) describes part of the constraints in a turtle file.

This library provides Python utilities for generating, validating, and testing CSVW-SAFE metadata and associated dummy datasets for differential privacy (DP) development and safe data modeling workflows.

It includes four main scripts:

1. make_metadata_from_data.py
2. make_dummy_from_metadata.py
3. validate_metadata.py
4. assert_same_structure.py

This is available in a pip library `csvw-safe-lib` described in [the README.md of `csvw-safe-lib`](https://github.com/dscc-admin-ch/csvw-safe/blob/main/csvw-safe-library/README.md).

![Overview](images/utils_scripts.png)


