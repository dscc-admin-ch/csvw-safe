# CSVW-SAFE Constraints

CSVW-SAFE enforces constraints to ensure both semantic correctness and DP validity. Constraints apply at table, column, multi-column group, and partition levels.

All constraints assume the recursive `csvw-safe:PartitionKey` / `csvw-safe:components` model.

### 4.1 Table-Level Constraints

Applied to `csvw:Table`:

| Property                                   | Constraint / Rule                                                 |
| ------------------------------------------ | ----------------------------------------------------------------- |
| `csvw-safe:publicLength` (if declared)     | Must be ≤ `csvw-safe:maxLength`                                   |
| `csvw-safe:maxLength`                      | Defines the global upper bound for the dataset (single partition) |
| `csvw-safe:maxContributions`                | ≤ `csvw-safe:maxLength`                                           |
| `csvw-safe:maxNumPartitions` (if declared) | Structural upper bound on grouping universe                       |


### 4.2 Column-Level Constraints

Applied to `csvw:Column` used as a grouping key:

| Rule                                                    | Meaning / Enforcement                                                    |
| ------------------------------------------------------- | ------------------------------------------------------------------------ |
| `csvw-safe:publicPartitions` values                     | Must match column datatype (`string`, `number`, etc.)                    |
| `csvw-safe:lowerBound ≤ csvw-safe:upperBound` (numeric) | Numeric partitions must have consistent bounds                           |
| `csvw-safe:lowerInclusive`, `csvw-safe:upperInclusive`  | Must be boolean if numeric bounds are declared                           |

Note: Optional columns may declare null fractions; this can affect `csvw-safe:maxLength` calculations.


### 4.3 Multi-Column Grouping Worst-Case Bounds

For `csvw-safe:GroupingKey` entities:

| Property                            | Worst-case derivation / Rule                                                                                        |
| ----------------------------------- | ------------------------------------------------------------------------------------------------------------------- |
| `csvw-safe:maxLength`               | ≤ `min(csvw-safe:maxLength)` of parent grouping scopes containing identical `privacyUnit`                           |
| `csvw-safe:maxNumPartitions`        | ≤ product of per-column `csvw-safe:maxNumPartitions`                                                                |
| `csvw-safe:maxGroupsPerUnit`        | ≤ `min(csvw-safe:maxGroupsPerUnit)` of parent grouping scopes containing identical `privacyUnit`             |
| `csvw-safe:maxContributions`        | ≤ `min(csvw-safe:maxContributions)` of parent grouping scopes containing identical `privacyUnit`                    |
| `csvw-safe:publicPartitions`        | Must represent a subset of the Cartesian product of per-column partitions, expressed via `csvw-safe:components`     |


Notes:
- Declaring csvw-safe:publicPartitions is only allowed if all columns in the group declare `csvw-safe:publicPartitions`.
- csvw-safe:components in each partition key must reference columns in csvw-safe:columns, and the referenced columns must exist in the table schema.

Additional Group-Level Rules
| Rule                                                                    | Meaning / Enforcement                                             |
| ----------------------------------------------------------------------- | ----------------------------------------------------------------- |
| If any column lacks `csvw-safe:publicPartitions`                        | The group **must not declare** `csvw-safe:publicPartitions`       |
| If any column lacks `csvw-safe:maxNumPartitions`                        | The group **must not declare** `csvw-safe:maxNumPartitions`       |
| `csvw-safe:components` keys must match `csvw-safe:columns`              | Structural consistency                                            |
| Partition values in `csvw-safe:components` must respect column datatype | Type correctness                                                  |
| Overrides of DP bounds in `csvw-safe:Partition`                      | Allowed but must be ≤ group-level DP bounds                       |


Notes:
- The recursion of `csvw-safe:Partition` ensures both categorical and numeric dimensions are validated consistently.
- Each `csvw-safe:Partition` in `csvw-safe:components` inherits bounds from the parent unless explicitly overridden.


### 4.4 Partition-Level Constraints

Applied to `csvw-safe:Partition`:

| Rule                                                                                                | Meaning / Enforcement                                  |
| --------------------------------------------------------------------------------------------------- | ------------------------------------------------------ |
| Structural partition represents a single group                                                      | Implicit `csvw-safe:maxNumPartitions = 1`              |
| `csvw-safe:components` keys must match parent grouping columns                                      | Structural consistency                                 |
| Categorical partitions must declare `csvw-safe:partitionValue`                                      | Required for categorical columns                       |
| Numeric partitions must declare `csvw-safe:lowerBound` and `csvw-safe:upperBound`                   | Required for numeric columns                           |
| Numeric bounds must satisfy `lowerBound ≤ upperBound`                                               | Interval validity                                      |
| DP bounds (`csvw-safe:maxLength`, `csvw-safe:maxGroupsPerUnit`, `csvw-safe:maxContributions`)       | Must be ≤ bounds declared at parent grouping key level |
| `csvw-safe:publicLength` (if declared)                                                              | Must be ≤ `csvw-safe:maxLength`                        |


> SHACL enforcement for all levels: [`csvw-safe-constaints.ttl`](https://github.com/dscc-admin-ch/csvw-safe/blob/main/csvw-safe-constaints.ttl)