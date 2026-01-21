# Library mapping (tentative)

> Terminology varies across DP frameworks.
> PU: Privacy Unit

| Concept / Role             | OpenDP                      | SmartNoise SQL| PipelineDP                      | Tumult Analytics   | ZetaSQL        | Vocabulary term    | Already defined? |
|----------------------------|-----------------------------|---------------|---------------------------------|--------------------|----------------|-------------------------------|---------|
| Table max length           | margin max_length           | —             | —                               | —                  | —              | `maxTableLength`             | new  |
| Table size (if known)      | margin length invariant     | n_row         | —                               | —                  | —              | `tableLength`                | new  |
| Max contribution per PU    | privacy_unit contribution   | max_ids       | max_contribution                | MaxRowsPerID       | —              | `maxContributions`           | new  |
| Column datatype            | ColumnDomain                | type          | —                               | —                  | —              | `datatype`                   | ✔ CSVW  |
| Privacy ID column          | —                           | private_id    | privacy_id                      | id_column          | privacy_unit   | `privacyId`                  | new  |
| Nullability                | —                           | nullable      | —                               | —                  | —              | `nullable`                   | ✔ CSVW-equivalent |
| Default / missing          | —                           | missing_value | —                               | —                  | —              | `default`                    | ✔ CSVW  |
| Bounds lower               | lower                       | lower         | min_value                       | low                | —              | `minimum`                    | ✔ CSVW  |
| Bounds upper               | upper                       | upper         | max_value                       | high               | —              | `maximum`                    | ✔ CSVW  |
| Public partitions key list | with_keys, margin keys invariant | —        | partition_key                   | keyset             | partition key  | `publicPartitions`           | new  |
| Partition max length           | max_partition_length         | —        | —                               | —                  | —              | `maxPartitionLength`         | new  |
| Max number of partition per PU | max_influenced_partitions    | —        | max_partition_contributed       | MaxGroupsPerID     | max_groups_contributed | `maxInfluencedPartitions` | new  |
| Max PU per partition           | max_partition_contribution   | —        | max_contributions_per_partition | MaxRowsPerGroupPerID | —          | `maxPartitionContribution`     | new  |
| Max number of partition    | max_group                   | —             | max_partitions                  | —                  | (1)            | `maxNumPartitions`           | new  |

(1): contribution_bounds_per_group: (max_contribution_per_partition*bounds)