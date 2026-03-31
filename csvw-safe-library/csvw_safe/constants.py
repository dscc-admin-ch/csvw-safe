"""Defaults, constants and metadata objects for csvw-safe."""

import string
from enum import StrEnum
from pathlib import Path

# ============================================================
# CSVW
# ============================================================
CSVW_CONTEXT = "http://www.w3.org/ns/csvw"
COL_NAME = "name"
DATATYPE = "datatype"
REQUIRED = "required"
MINIMUM = "minimum"
MAXIMUM = "maximum"
TABLE_SCHEMA = "tableSchema"
COL_LIST = "columns"
COL_TYPE = "Column"
TABLE_TYPE = "Table"

# ============================================================
# CSVW_SAFE Namespaces
# ============================================================
CSVW_SAFE_CONTEXT = str(
    (Path(__file__).resolve().parents[2] / "csvw-safe-context.jsonld").resolve()
)  # tmp

# Column groups / partitions
COLUMN_GROUP = "ColumnGroup"
PARTITION = "Partition"
COLUMNS_IN_GROUP = "columnsInGroup"
PUBLIC_PARTITIONS = "partitions"
PUBLIC_KEYS = "keys"
EXHAUSTIVE_PARTITIONS = "exhaustivePartitions"
MAX_NUM_PARTITIONS = "maxNumPartitions"
PUBLIC_LENGTH = "length"
PRIVACY_UNIT = "privacyUnit"
PRIVACY_ID = "privacyId"
ADD_INFO = "additionalInformation"

# Differential privacy bounds
MAX_LENGTH = "maxLength"
MAX_GROUPS = "maxGroupsPerUnit"
MAX_CONTRIB = "maxContributions"

# Partition predicates
PREDICATE = "predicate"
PARTITION_VALUE = "partitionValue"
LOWER_BOUND = "lowerBound"
UPPER_BOUND = "upperBound"

# Synthetic modeling
NULL_PROP = "nullableProportion"
ROW_DEP = "rowDependencies"
DEPENDS_ON = "dependsOn"
DEPENDENCY_TYPE = "dependencyType"
VALUE_MAP = "valueMap"


# ============================================================
# Make and generate metadata
# ============================================================
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
RANDOM_STRINGS = list(string.ascii_lowercase + string.ascii_uppercase + string.digits)
