# CSVW Differential Privacy Extension Vocabulary

## Overview

The **CSVW Differential Privacy Extension (CSVW-DP)** is a vocabulary designed to complement the W3C [CSV on the Web](https://www.w3.org/TR/tabular-data-model/) metadata model.

It defines terms needed to express bounded influence assumptions about individuals in tabular data — the assumptions most differential privacy (DP) systems require but CSVW cannot describe today.

## Motivation

Differential privacy libraries assume (and often require) metadata such as:

- `max_contributions` per person  
- maximum group sizes
- bounds on how many partitions a person touches  
- constraints that prevent overflow or numerical instability during aggregation

These assumptions are essential for meaningful DP guarantees, but the core CSVW vocabulary cannot express them.

CSVW-DP introduces new terms so that a dataset can explicitly declare:

- table-wide DP contribution bounds  
- per-column DP roles and limits  
- multi-column grouping assumptions


The vocabulary intentionally avoids descriptive notions such as 'cardinality' or 'categories' in favor of DP-semantic bounds.

---

## Namespace

**Default namespace:** https://github.com/dscc-admin-ch/csvw-dp/csvw-dp-ext#


Machine-readable definitions live in: csvw-dp-ext.ttl

### Table-level properties

| Term | Type | Meaning |
|------|------|---------|
| `dp:maxTableLength` | positive integer | Upper bound on total rows (used to avoid overflow / numerical instability). |
| `dp:tableLength` | positive integer | Number of rows in table (if known). |
| `dp:maxContributions` | positive integer | Max number of rows per individual (≈ L0 bound). |


### Column-level properties

| Term | Type | Meaning |
|------|------|---------|
| `dp:privacyId` | boolean | True if the column identifies individuals/units for DP. |
| `dp:nullable` | boolean | Whether null values may appear. |
| `dp:nullableProportion` | decimal 0–1 | Fraction of values that are null. |
| `dp:publicPartitions` | list(string) | Declared category set if partitions are known ahead of time. |


### Groupable

CSVW-DP introduces an abstract helper class:

dp:Groupable

dp:Groupable represents any entity that can be used to form groups (partitions) for aggregation and differential privacy analysis.

It is not instantiated directly.
Instead, two concrete CSVW concepts specialize it:

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

CSVW-DP introduces a helper class:

dp:ColumnGroup

Represents a grouping key formed by two or more columns, or a single column reused in a composite context.

| Term | Applies to | Meaning |
|------|------------|---------|
| `dp:columns` | ColumnGroup | List of columns that jointly define the composite key. |
| `dp:maxPartitionLength` | ColumnGroup | Max size of any group when grouping by the column. |
| `dp:maxNumPartitions` | ColumnGroup | Max number of distinct groups keys. |
| `dp:maxInfluencedPartitions` | ColumnGroup | Max number of groups a person may contribute to. |
| `dp:maxPartitionContribution` | ColumnGroup | Max contributions inside one partition. |

DP properties on dp:ColumnGroup reuse the same terms as columns (dp:maxPartitionLength, etc.), since both are subclasses of dp:Groupable.


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
        │        ├─ dp:nullable                 : xsd:boolean
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

## Library mapping (tentative)

> Terminology varies across DP frameworks.
> PU: Privacy Unit

| Concept / Role             | OpenDP                      | SmartNoise SQL| PipelineDP                      | Tumult Analytics   | ZetaSQL        | Vocabulary term    | Already defined? |
|----------------------------|-----------------------------|---------------|---------------------------------|--------------------|----------------|-------------------------------|---------|
| Table max length           | margin max_length           | —             | —                               | —                  | —              | `maxTableLength`             | ❌ new  |
| Table size (if known)      | margin length invariant     | n_row         | —                               | —                  | —              | `tableLength`                | ❌ new  |
| Max contribution per PU    | privacy_unit contribution   | max_ids       | max_contribution                | MaxRowsPerID       | —              | `maxContributions`           | ❌ new  |
| Column datatype            | ColumnDomain                | type          | —                               | —                  | —              | `datatype`                   | ✔ CSVW  |
| Privacy ID column          | —                           | private_id    | privacy_id                      | id_column          | privacy_unit   | `privacyId`                  | ❌ new  |
| Nullability                | —                           | nullable      | —                               | —                  | —              | `nullable`                   | ✔ CSVW-equivalent |
| Default / missing          | —                           | missing_value | —                               | —                  | —              | `default`                    | ✔ CSVW  |
| Bounds lower               | lower                       | lower         | min_value                       | low                | —              | `minimum`                    | ✔ CSVW  |
| Bounds upper               | upper                       | upper         | max_value                       | high               | —              | `maximum`                    | ✔ CSVW  |
| Public partitions key list | with_keys, margin keys invariant | —        | partition_key                   | keyset             | partition key  | `publicPartitions`           | ❌ new  |
| Partition max length           | max_partition_length         | —        | —                               | —                  | —              | `maxPartitionLength`         | ❌ new  |
| Max number of partition per PU | max_influenced_partitions    | —        | max_partition_contributed       | MaxGroupsPerID     | max_groups_contributed | `maxInfluencedPartitions` | ❌ new  |
| Max PU per partition           | max_partition_contribution   | —        | max_contributions_per_partition | MaxRowsPerGroupPerID | —          | `maxPartitionContribution`     | ❌ new  |
| Max number of partition    | max_group                   | —             | max_partitions                  | —                  | (1)            | `maxNumPartitions`           | ❌ new  |

(1): contribution_bounds_per_group: (max_contribution_per_partition*bounds)

---

## (Notes from) Guidelines from Open Data Support (European Commission)

[Designing and developing RDF vocabularies](https://data.europa.eu/sites/default/files/d2.1.2_training_module_2.4_designing_and_developing_vocabularies_in_rdf_en_edp.pdf)

RDF vocabulary: A vocabulary is a data model comprising classes, properties and relationships which can be used for describing your data and metadata.

RDF Vocabularies are sets of terms used to describe things.
A term is either a class or a property
- Object type properties (relationships)
- Data type properties (attributes) -- in our case, only attributes (To verify)

Properties begin with a lower case letter, e.g. rdfs:label.
Data type properties should be nouns, e.g. dcterms:description.
Use camel case if a term has more than one word, e.g. foaf:isPrimaryTopicOf

### Steps for modelling data
1. Start with a robust Domain Model developed following a structured process and methodology.
2. Research existing terms and their usage and maximise reuse of those terms. Reusable RDF vocabularies on [Linked Open Vocabulary](https://lov.linkeddata.es/dataset/lov/).
4. Where new terms can be seen as specialisations of existing terms, create sub class and sub properties.
5. Where new terms are required, create them following commonly agreed best practice.
6. Publish within a highly stable environment designed to be persistent. Choose a stable namespace for your RDF schema (e.g. W3C, Purl...). Use good practices on the publication of persistent Uniform  Resource Identifiers (URI) sets, both in terms of format and of their design rules and management.
7. Publicise the RDF schema by registering it with relevant services (Joinup and Linked Open Vocabularies).

See [this](https://interoperable-europe.ec.europa.eu/collection/semic-support-centre/document/process-and-methodology-developing-core-vocabularies).

### Already existing vocabulary:
['Privacy' in search engine](https://lov.linkeddata.es/dataset/lov/terms?q=privacy)

**DCAT**: Describing Dataset: recommends DCAT but here we mean something else. We want to describe the 'inside' of the dataset not the way to share it. 

**dcterms:accessRights**: access or restrictions based on privacy, security, or other policies

**dpv:DifferentialPrivacy**: https://w3c.github.io/dpv/2.2/dpv/#DifferentialPrivacy. But more legal, consent, residual risk, access control management. Also has a 'Data & Personal Data' part but more about is sensitive, confidential or other data.

Other vocabulary have fields about privacy but coarse and table level.


## SHACL Validation Rules (TODO)

### Nullability
- If `dp:nullable` is `true`, then `dp:nullableProportion` **MUST be greater than 0**.
- If `dp:nullable` is `false`, then `dp:nullableProportion` **MUST be 0** (if `dp:nullableProportion` is provided).


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


Standard SPARQL 1.1 can’t directly compute product() over a list of values. Need pySHACL.

## Status

It is a **work in progress** and subject to change.
To be used in Lomas.
