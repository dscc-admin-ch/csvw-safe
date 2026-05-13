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


## Cross-Library DP Vocabulary Mapping

| Type      | CSVW-EO Vocabulary | OpenDP                                         | Smartnoise-SQL  | Privacy on Beam | PipelineDP          |    Qrlew         | Tumult       |
|-----------|----------------------|------------------------------------------------|-----------------|-----------------|---------------------|------------------|--------------|
| Table     | maxContributions     | dp.unit_of(contributions=1)                    | max_ids         |                 | max_contribution    | max_multiplicity | MaxRowsPerID |
| Table     | publicLength         | margins=[dp.polars.Margin(invariant="length")] | n_row           |                 |                     | size             |              |
| Table     | maxLength            | margins=[dp.polars.Margin(max_length=100_000)] |                 |                 |                     |                  |              |
|           |                      |                                                |                 |                 |                     |                  |              |
| Column    | privacy_id           | dp.unit_of(identifier="XX")                    | private_id      |                       | privacy_id    | PrimaryKey       | id_column    |
| Column    | minimum              | bounds in polars query                         | lower           | MinValue              | min_value     | with_range, min  | low          |
| Column    | maximum              | bounds in polars query                         | upper           | MaxValue              | max_value     | with_range, max  | high         |
|           |                      |                                                |                 |                       |               |                  |              |
| Column & ColumnGroup | invariantPublicKeys | margins=[by=list_of_cols, invariant="keys"] |          |                       |               |                  |              |
| Column & ColumnGroup | keyValues          | with_keys() operation                       |          |PublicPartitions       | partition_key | with_possible_values | keyset   |
| Column & ColumnGroup | maxLength           | margins=[by=list_of_cols, max_length=150_000]|         |                       |               |                  |              |
| Column & ColumnGroup | publicLength        | margins=[by=list_of_cols, invariant="length"]|         |                       |               |                  |              |
| Column & ColumnGroup | maxContributions    | dp.unit_of(contributions=[Bound(...)]) (?) |    | MaxContributionsPerPartition | max_contributions_per_partition| | MaxRowsPerGroupPerID |
| Column & ColumnGroup | maxGroupsPerUnit    | margins=[by=list_of_cols, max_groups=3]  |          | MaxPartitionsContributed | max_partitions_contributed     | | MaxGroupsPerID |
|           |                     |                                                 |                 |                       |               |                  |              |
| Partition | maxLength           | unsure (?)                                      |                 |                       |               |                  |              |
| Partition | publicLength        | unsure (?)                                      |                 |                       |               |                  |              |
| Partition | maxContributions    | dp.unit_of(contributions=[Bound(...)])          |                 |                       |               |                  |              |

Other library mappings:
- DiffPrivLib only have minimum and maximum equivalent with lower and upper respectivelly (or data_bounds).
- Smartnoise-Synth only have minimum, maximum and privacy id equivalent with bounds and pii_data respectivelly.
- PyDP only have minimum, maximum and maxContributions equivalent with bounds (or sensitivity) and max_contributions respectivelly.
- ZetaSQL only have privacy_unit_column, max_groups_contributed and contribution_bounds_per_group equivalent with privacy_id, maxGroupsPerUnit and maxContributions respectivelly.
