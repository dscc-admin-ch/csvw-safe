# Library mapping (tentative)

> Terminology varies across DP frameworks.
> PU: Privacy Unit

## Libraries

| DP Library | GitHub / Docs | Belongs to | Forks | Stars | Last Updated | Language | Description |
|-----------|---------------|------------|-------|-------|--------------|----------|-------------|
| DiffPrivLib | Welcome to the IBM Differential Privacy Library — Diffprivlib | IBM | 205 | 899 | 9 months ago | Python | DP statistics + DP ML (scikit-learn–like). |
| OpenDP | Quickstart — OpenDP | Harvard | 64 | 402 | recent | Rust (+ Python / R) | Core library of DP algorithms, transformations, privacy pipelines. |
| SmartNoise-SQL | OpenDP SmartNoise SQL | Microsoft + Harvard | 76 / 2 | 290 / 2 | last year | Python / SQL | DP SQL interface that wraps DB connections, intercepts queries, enforces DP. |
| SmartNoise-Synth | OpenDP SmartNoise Synthesizers | Microsoft + Harvard | 76 / 2 | 290 / 2 | 9 months ago | Python | Synthetic data generation under DP. |
| Google Differential Privacy (Building Blocks) | google/differential-privacy | Google | 399 | 3.2k | recent | Java, Go, C++ | Noise addition and basic aggregations. |
| Google Differential Privacy (Beam) | google/differential-privacy | Google | 399 | 3.2k | recent | Go | Privacy on Beam: DP for Go with Apache Beam. |
| Google Differential Privacy (JVM) | google/differential-privacy | Google | 399 | 3.2k | recent | JVM (Java, Kotlin, Scala) | PipelineDP4j: DP for JVM with Apache Beam and Spark. |
| Google Differential Privacy (SQL) | google/differential-privacy | Google | 399 | 3.2k | recent | SQL CLI | ZetaSQL: DP SQL queries with DP instructions inside queries. |
| Google Differential Privacy (Accounting) | google/differential-privacy | Google | 399 | 3.2k | recent | Python | DP accounting library. |
| Google Differential Privacy (Auditing) | google/differential-privacy | Google | 399 | 3.2k | recent | Python | DP Auditorium: black-box auditing of DP guarantees via hypothesis testing. |
| PyDP | OpenMined/PyDP | OpenMined / Google | 142 | 541 | recent | Python (wrapper) | Python bindings for Google DP C++ (count, sum, mean, quantile, sklearn-style). |
| PipelineDP | OpenMined/PipelineDP | OpenMined / Google | 85 | 283 | recent | Python | Backend-agnostic PipelineDP (Python version of PipelineDP4j). |
| TensorFlow Privacy | tensorflow/privacy | Google | 467 | 2k | recent | Python / TensorFlow | DP training (DP-SGD) for TensorFlow / Keras models. |
| PyTorch Opacus | Opacus | Meta | 389 | 1.9k | recent | Python / PyTorch | DP training (DP-SGD) for PyTorch models. |
| JAX Privacy | google-deepmind/jax_privacy | Google DeepMind | 33 | 141 | recent | Python / JAX / Keras | Differentially private machine learning. |
| Tumult Analytics | opendp/tumult-analytics | OpenDP | NA | NA | recent | Python | Private aggregate queries on tabular data; joins, maps, flatmaps (Python 3.9–3.11). |
| PyQrlew | pyqrlew.readthedocs.io | Sarus | — | 48 | 2 years ago | Python / SQL | DP SQL engine with separate metadata, joins, and many SQL backends. |


## Mapping

| Concept / Role             | OpenDP                      | SmartNoise SQL| PipelineDP                      | Tumult Analytics   | ZetaSQL        | Vocabulary term    | Already defined? |
|----------------------------|-----------------------------|---------------|---------------------------------|--------------------|----------------|-------------------------------|---------|
| Table max length           | margin max_length           | —             | —                               | —                  | —              | `maxTableLength`             | new  |
| Table size (if known)      | margin length invariant     | n_row         | —                               | —                  | —              | `tableLength`                | new  |
| Max contribution per PU    | privacy_unit contribution   | max_ids       | max_contribution                | MaxRowsPerID       | —              | `maxContributions`           | new  |
| Column datatype            | ColumnDomain                | type          | —                               | —                  | —              | `datatype`                   | CSVW  |
| Privacy ID column          | —                           | private_id    | privacy_id                      | id_column          | privacy_unit   | `privacyId`                  | new  |
| Nullability                | —                           | nullable      | —                               | —                  | —              | `required`                   | CSVW  |
| Default / missing          | —                           | missing_value | —                               | —                  | —              | `default`                    | CSVW  |
| Bounds lower               | lower                       | lower         | min_value                       | low                | —              | `minimum`                    | CSVW  |
| Bounds upper               | upper                       | upper         | max_value                       | high               | —              | `maximum`                    | CSVW  |
| Public partitions key list | with_keys, margin keys invariant | —        | partition_key                   | keyset             | partition key  | `publicPartitions`           | new  |
| Partition max length       | max_partition_length        | —             | —                               | —                  | —              | `maxPartitionLength`         | new  |
| Max of partition per PU    | max_influenced_partitions   | —             | max_partition_contributed       | MaxGroupsPerID     | max_groups_contributed | `maxInfluencedPartitions` | new  |
| Max PU per partition       | max_partition_contribution  | —             | max_contributions_per_partition | MaxRowsPerGroupPerID | —          | `maxPartitionContribution`     | new  |
| Max number of partition    | max_group                   | —             | max_partitions                  | —                  | (1)            | `maxNumPartitions`           | new  |

(1): contribution_bounds_per_group: (max_contribution_per_partition*bounds)

| Concept / Role             | Privacy on Beam   | SmartNoise Synth| Qrlew                | DiffPrivLib        | Vocabulary term  | Already defined? |
|----------------------------|-------------------|-----------------|----------------------|--------------------|------------------|------------------|
| Table max length           | —                 | —               | —                    | —                  | `maxTableLength`             | new  |
| Table size (if known)      | —                 | —               | size                 | —                  | `tableLength`                | new  |
| Max contribution per PU    | —                 | —               | max_multiplicity     | —                  | `maxContributions`           | new  |
| Column datatype            | —                 | in constraints  | data_type            | —                  | `datatype`                   | CSVW |
| Privacy ID column          | —                 | pii_data        | PrimaryKey (?)       | —                  | `privacyId`                  | new  |
| Nullability                | —                 | nullable        | —                    | —                  | `required`                   | CSVW |
| Default / missing          | —                 | —               | —                    | —                  | `default`                    | CSVW |
| Bounds lower               | MinValue          | lower           | with range, min      | bounds / data_norm | `minimum`                    | CSVW |
| Bounds upper               | MaxValue          | upper           | with range, max      | bounds / data_norm | `maximum`                    | CSVW |
| Public partitions key list | PublicPartitions  | —               | with_possible_values | —                  | `publicPartitions`           | new  |
| Partition max length       | —                 | —               | —                    | —                  | `maxPartitionLength`         | new  |
| Max of partition per PU    | MaxPartitionsContributed | —        | —                    | —                  | `maxInfluencedPartitions`    | new  |
| Max PU per partition       | MaxContributionsPerPartition | —    | —                    | —                  | `maxPartitionContribution`   | new  |
| Max number of partition    | —                    | —            | —                    | —                  | `maxNumPartitions`           | new  |


