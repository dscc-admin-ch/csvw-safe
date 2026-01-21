# Library mapping (tentative)

> Terminology varies across DP frameworks.
> PU: Privacy Unit

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
