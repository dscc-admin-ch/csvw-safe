"""Defaults, constants and metadata objects for csvw-safe."""

from enum import Enum

# ============================================================
# Namespaces
# ============================================================
CSVW_SAFE = "csvw-safe"

# ============================================================
# Column groups / partitions
# ============================================================
COLUMN_GROUP = f"{CSVW_SAFE}:ColumnGroup"
PARTITION = f"{CSVW_SAFE}:Partition"

COLUMNS = f"{CSVW_SAFE}:columns"
PUBLIC_PARTITIONS = f"{CSVW_SAFE}:partitions"
MAX_NUM_PARTITIONS = f"{CSVW_SAFE}:maxNumPartitions"

PUBLIC_LENGTH = f"{CSVW_SAFE}:length"
PRIVACY_UNIT = f"{CSVW_SAFE}:privacyUnit"
PRIVACY_ID = f"{CSVW_SAFE}:privacyId"
ADD_INFO = f"{CSVW_SAFE}:additionalInformation"


# ============================================================
# Differential privacy bounds
# ============================================================
DP = f"{CSVW_SAFE}:dp"

MAX_LENGTH = f"{DP}.maxLength"
MAX_GROUPS = f"{DP}.maxGroupsPerUnit"
MAX_CONTRIB = f"{DP}.maxContributions"

# ============================================================
# Partition predicates
# ============================================================
PARTS = f"{CSVW_SAFE}:part"

PREDICATE = f"{PARTS}.predicate"
PARTITION_VALUE = f"{PARTS}.partitionValue"
LOWER_BOUND = f"{PARTS}.lowerBound"
UPPER_BOUND = f"{PARTS}.upperBound"

# ============================================================
# Synthetic modeling
# ============================================================
SYNTH = f"{CSVW_SAFE}:synth"

NULL_PROP = f"{SYNTH}.nullableProportion"
ROW_DEP = f"{SYNTH}.rowDependencies"

DEPENDS_ON = f"{SYNTH}.dependsOn"
DEPENDENCY_TYPE = f"{SYNTH}.dependency_type"
VALUE_MAP = f"{SYNTH}.value_map"

FIXED_PER_ENTITY = f"{SYNTH}.fixed_per_entity"


class DependencyType(str, Enum):
    """Types of column dependency relationships."""

    MAPPING = "mapping"
    BIGGER = "bigger"
    SMALLER = "smaller"


# ============================================================
# Default bounds
# ============================================================
DEFAULT_LOWER_INCLUSIVE = True
DEFAULT_UPPER_INCLUSIVE = True
