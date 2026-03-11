"""
Validates table-level, column-level, partition-level, and column group metadata.

according to CSVW-SAFE conventions.
"""

import argparse
import json
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional

from csvw_safe.constants import (
    ADD_INFO,
    COLUMN_GROUP,
    COLUMNS,
    LOWER_BOUND,
    MAX_CONTRIB,
    MAX_LENGTH,
    MAX_NUM_PARTITIONS,
    NULL_PROP,
    PARTITION,
    PARTITION_VALUE,
    PREDICATE,
    PRIVACY_ID,
    PRIVACY_UNIT,
    PUBLIC_LENGTH,
    PUBLIC_PARTITIONS,
    UPPER_BOUND,
)
from csvw_safe.datatypes import DataTypes


def validate_table(metadata: Dict[str, Any]) -> None:
    """Validate CSVW-SAFE table-level fields."""
    if PRIVACY_UNIT not in metadata:
        raise ValueError(f"Missing {PRIVACY_UNIT} at table level")

    bounds_max_len = metadata.get(MAX_LENGTH)
    public_len = metadata.get(PUBLIC_LENGTH)

    if isinstance(bounds_max_len, (int, float)) and isinstance(public_len, (int, float)):
        if public_len > bounds_max_len:
            raise ValueError(f"{PUBLIC_LENGTH} exceeds {MAX_LENGTH}")

    if MAX_CONTRIB not in metadata:
        raise ValueError(f"Missing {MAX_CONTRIB} at table level")

    if metadata[MAX_CONTRIB] < 1:
        raise ValueError(f"{MAX_CONTRIB} must be >= 1")


def validate_column(col: Dict[str, Any]) -> None:
    """Validate a single column."""
    name = col.get("name")
    if not name:
        raise ValueError("Column missing 'name'")

    dtype = col.get("datatype")
    if dtype not in {t.value for t in DataTypes}:
        raise ValueError(f"Column '{name}' invalid datatype '{dtype}'")

    privacy_id = col.get(PRIVACY_ID)
    if privacy_id is not None and not isinstance(privacy_id, bool):
        raise ValueError(f"Column '{name}' privacyId must be boolean")

    required = col.get("required", False)
    nullable_prop = col.get(NULL_PROP, 0)
    if required and nullable_prop != 0:
        raise ValueError(f"Column '{name}' is required but nullableProportion != 0")

    if dtype in ("integer", "double"):
        minimum = col.get("minimum")
        maximum = col.get("maximum")
        if minimum is None or maximum is None:
            raise ValueError(f"Numeric column '{name}' must declare minimum and maximum")
        if (
            isinstance(minimum, (int, float))
            and isinstance(maximum, (int, float))
            and minimum > maximum
        ):
            raise ValueError(f"Column '{name}' minimum > maximum")

    partitions = col.get(PUBLIC_PARTITIONS)
    if isinstance(partitions, list):
        validate_partitions(col, partitions)
        max_num = col.get(MAX_NUM_PARTITIONS)
        if isinstance(max_num, int) and len(partitions) > max_num:
            raise ValueError(f"Column '{name}' exceeds declared {MAX_NUM_PARTITIONS}")


def validate_partition_predicate(predicate: dict[str, Any], col_name: Optional[str] = None) -> None:
    """Validate a single predicate object and return numeric bounds if present."""
    has_value = PARTITION_VALUE in predicate
    has_bounds = LOWER_BOUND in predicate or UPPER_BOUND in predicate

    if not (has_value or has_bounds):
        col = f" for '{col_name}'" if col_name else ""
        raise ValueError(f"Partition predicate{col} must define partitionValue or bounds")

    if has_bounds:
        lb = predicate.get(LOWER_BOUND)
        ub = predicate.get(UPPER_BOUND)
        if lb is None or ub is None:
            raise ValueError(
                f"Continuous partition{f' for {col_name}' if col_name else ''} "
                f"must define lowerBound and upperBound"
            )
        if lb > ub:
            raise ValueError(
                f"Partition lowerBound > upperBound{f' for {col_name}' if col_name else ''}"
            )


def validate_single_partition(p: Dict[str, Any], is_column_group: bool) -> None:
    """Validate a single partition and return a list of numeric intervals for overlap checking."""
    predicate: Any = p.get(PREDICATE)
    if predicate is None:
        raise ValueError("Partition missing predicate object")

    if is_column_group:
        if not isinstance(predicate, dict):
            raise ValueError("ColumnGroup partition predicate must be a dict mapping columns")
        for col_name, value_raw in predicate.items():
            if not isinstance(value_raw, dict):
                raise ValueError(f"ColumnGroup partition predicate for '{col_name}' must be object")
            validate_partition_predicate(value_raw, col_name)
    else:
        if isinstance(predicate, dict):
            validate_partition_predicate(predicate)


def validate_partitions(parent: Dict[str, Any], partitions: List[Dict[str, Any]]) -> None:
    """Validate partitions for columns or column groups in CSVW-SAFE metadata."""
    is_column_group = parent.get("@type") == COLUMN_GROUP
    for p in partitions:
        # validate @type for single-column partitions
        if not is_column_group and "@type" in p and p.get("@type") != PARTITION:
            raise ValueError(f"Partition must declare @type {PARTITION}")

        validate_single_partition(p, is_column_group)


def validate_column_groups(metadata: Dict[str, Any], columns_by_name: Dict[str, Any]) -> None:
    """Validate ColumnGroup entries."""
    groups = metadata.get(ADD_INFO, [])
    if not isinstance(groups, list):
        return

    for g in groups:
        if g.get("@type") != COLUMN_GROUP:
            continue
        cols = g.get(COLUMNS, [])
        if not isinstance(cols, list):
            continue
        if len(cols) < 2:
            raise ValueError("ColumnGroup must reference at least two columns")
        for col_name in cols:
            if col_name not in columns_by_name:
                raise ValueError(f"ColumnGroup references unknown column '{col_name}'")
            if columns_by_name[col_name].get(PRIVACY_ID):
                raise ValueError(f"ColumnGroup cannot include privacyId column '{col_name}'")
        partitions = g.get(PUBLIC_PARTITIONS)
        if isinstance(partitions, list):
            validate_partitions(g, partitions)
        max_num = g.get(MAX_NUM_PARTITIONS)
        if isinstance(max_num, int) and isinstance(partitions, list) and len(partitions) > max_num:
            raise ValueError("ColumnGroup exceeds public.maxNumPartitions")


def validate_metadata(metadata: Dict[str, Any]) -> None:
    """Validate full CSVW-SAFE metadata."""
    validate_table(metadata)

    table_schema = metadata.get("csvw:tableSchema", {})
    columns = table_schema.get("columns", [])

    columns_by_name: Dict[str, Dict[str, Any]] = {}
    for col in columns:
        if isinstance(col, dict):
            name = col.get("name")
            if name:
                columns_by_name[name] = col
            validate_column(col)

    validate_column_groups(metadata, columns_by_name)


def main() -> None:
    """Validate CSVW-SAFE metadata."""
    parser = argparse.ArgumentParser(description="Validate CSVW-SAFE metadata")
    parser.add_argument("metadata_file", type=str)
    args = parser.parse_args()

    metadata_path = Path(args.metadata_file)
    if not metadata_path.exists():
        print(f"File not found: {metadata_path}")
        sys.exit(1)

    with metadata_path.open("r", encoding="utf-8") as f:
        metadata = json.load(f)

    validate_metadata(metadata)
    print("VALIDATION SUCCESS: Metadata satisfies CSVW-SAFE specification")


if __name__ == "__main__":
    main()
