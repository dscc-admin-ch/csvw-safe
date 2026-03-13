"""Defaults, constants and metadata objects for csvw-safe."""

from enum import StrEnum

# ============================================================
# CSVW
# ============================================================
CSVW = "csvw"
COL_NAME = f"{CSVW}:name"
DATATYPE = f"{CSVW}:datatype"
REQUIRED = f"{CSVW}:required"
MINIMUM = f"{CSVW}:minimum"
MAXIMUM = f"{CSVW}:maximum"
TABLE_SCHEMA = f"{CSVW}:tableSchema"
COL_LIST = f"{CSVW}:columns"
COL_TYPE = f"{CSVW}:Column"
TABLE_TYPE = f"{CSVW}:Table"

CSVW_CONTEXT = "http://www.w3.org/ns/csvw"

# ============================================================
# CSVW_SAFE Namespaces
# ============================================================
CSVW_SAFE = "csvw-safe"
CSVW_SAFE_CONTEXT = "../../../csvw-safe-context.jsonld"

# ============================================================
# Column groups / partitions
# ============================================================
COLUMN_GROUP = f"{CSVW_SAFE}:ColumnGroup"
PARTITION = f"{CSVW_SAFE}:Partition"

COLUMNS = f"{CSVW_SAFE}:columns"
PUBLIC_PARTITIONS = f"{CSVW_SAFE}:partitions"
PUBLIC_KEYS = f"{CSVW_SAFE}:keys"
EXHAUSTIVE_PARTITIONS = f"{CSVW_SAFE}:exhaustivePartitions"
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
DEPENDENCY_TYPE = f"{SYNTH}.dependencyType"
VALUE_MAP = f"{SYNTH}.valueMap"


class DependencyType(StrEnum):
    """Types of column dependency relationships."""

    MAPPING = "mapping"
    BIGGER = "bigger"
    # SMALLER = "smaller"  # redundant with bigger
    FIXED = "fixedPerEntity"


# ============================================================
# Default Values
# ============================================================
DEFAULT_LOWER_INCLUSIVE = True
DEFAULT_UPPER_INCLUSIVE = True

DEFAULT_NUMBER_PARTITIONS = 10
OVERSAMPLING_FACTOR = 1.5
