"""
Validates table-level, column-level, partition-level, and column group metadata
according to CSVW-SAFE conventions.
"""

import argparse
import json
import sys
from pathlib import Path
from typing import Any, Dict, List, Tuple, Union

# Allowed datatypes
VALID_TYPES = {"string", "boolean", "decimal", "double", "dateTime"}
NumericType = Union[int, float]
PredicateType = Union[Dict[str, Any], int, float, str]


# ============================================================
# Utilities
# ============================================================
def error(msg: str, errors: List[str]) -> None:
    """Append an error message."""
    errors.append(msg)


def is_numeric(dtype: str) -> bool:
    """Check if a datatype is numeric."""
    return dtype in ("integer", "double")


def intervals_overlap(a1: NumericType, a2: NumericType, b1: NumericType, b2: NumericType) -> bool:
    """Check if two numeric intervals overlap."""
    return max(a1, b1) < min(a2, b2)


def map_validator_type(datatype: Any, col_meta: Dict[str, Any]) -> str:
    """Map generator datatypes to validator-compatible types."""
    # mypy-safe cast to str
    dtype_str = str(datatype) if datatype is not None else "string"
    if dtype_str == "decimal":
        minimum = col_meta.get("minimum")
        maximum = col_meta.get("maximum")
        if isinstance(minimum, (int, float)) and isinstance(maximum, (int, float)):
            if float(minimum).is_integer() and float(maximum).is_integer():
                return "decimal"
        return "double"
    if dtype_str in VALID_TYPES:
        return dtype_str
    return "string"


# ============================================================
# Table-level validation
# ============================================================
def validate_table(metadata: Dict[str, Any], errors: List[str]) -> None:
    """Validate CSVW-SAFE table-level fields."""
    if "csvw-safe:public.privacyUnit" not in metadata:
        error("Missing csvw-safe:public.privacyUnit at table level", errors)

    bounds_max_len = metadata.get("csvw-safe:bounds.maxLength")
    public_len = metadata.get("csvw-safe:public.length")

    if isinstance(bounds_max_len, (int, float)) and isinstance(public_len, (int, float)):
        if public_len > bounds_max_len:
            error("public.length exceeds bounds.maxLength", errors)

    max_contrib = metadata.get("csvw-safe:bounds.maxContributions")
    if isinstance(max_contrib, (int, float)) and max_contrib < 1:
        error("bounds.maxContributions must be >= 1", errors)


# ============================================================
# Column-level validation
# ============================================================
def validate_column(col: Dict[str, Any], errors: List[str]) -> None:
    """Validate a single column."""
    name = col.get("name")
    dtype = map_validator_type(col.get("datatype"), col)

    if not name:
        error("Column missing 'name'", errors)
        return

    if dtype not in VALID_TYPES:
        error(f"Column '{name}' invalid datatype '{dtype}'", errors)

    privacy_id = col.get("csvw-safe:public.privacyId")
    if privacy_id is not None and not isinstance(privacy_id, bool):
        error(f"Column '{name}' privacyId must be boolean", errors)

    required = col.get("required", False)
    nullable_prop = col.get("csvw-safe:synth.nullableProportion", 0)
    if required and nullable_prop != 0:
        error(f"Column '{name}' is required but nullableProportion != 0", errors)

    if is_numeric(dtype):
        minimum = col.get("minimum")
        maximum = col.get("maximum")
        if minimum is None or maximum is None:
            error(f"Numeric column '{name}' must declare minimum and maximum", errors)
        elif isinstance(minimum, (int, float)) and isinstance(maximum, (int, float)) and minimum > maximum:
            error(f"Column '{name}' minimum > maximum", errors)

    partitions = col.get("csvw-safe:public.partitions")
    if isinstance(partitions, list):
        validate_partitions(col, partitions, errors)
        max_num = col.get("csvw-safe:public.maxNumPartitions")
        if isinstance(max_num, int) and len(partitions) > max_num:
            error(f"Column '{name}' exceeds declared public.maxNumPartitions", errors)


# ============================================================
# Partition-level validation
# ============================================================
def validate_partitions(parent: Dict[str, Any], partitions: List[Dict[str, Any]], errors: List[str]) -> None:
    """
    Validate partitions for columns or column groups in CSVW-SAFE metadata.

    Parameters
    ----------
    parent : Dict[str, Any]
        The parent column or column group metadata containing partitions.
    partitions : List[Dict[str, Any]]
        List of partition objects.
    errors : List[str]
        Accumulates validation error messages.

    Notes
    -----
    - ColumnGroup partitions have predicates mapping column names to dicts with either
      'partitionValue' or bounds ('lowerBound'/'upperBound').
    - Single-column partitions can be primitive values or dicts with 'partitionValue' or bounds.
    - Overlapping numeric partitions are flagged as errors.
    """
    is_column_group = parent.get("@type") == "csvw-safe:ColumnGroup"
    numeric_intervals: List[Tuple[NumericType, NumericType]] = []

    for p in partitions:
        # Validate partition type for non-column-group
        if not is_column_group and "@type" in p and p.get("@type") != "csvw-safe:Partition":
            error("Partition must declare @type csvw-safe:Partition", errors)

        # Safely get predicate with type check
        predicate_raw: Any = p.get("csvw-safe:predicate")
        if predicate_raw is None:
            error("Partition missing predicate object", errors)
            continue
        if isinstance(predicate_raw, (dict, int, float, str)):
            predicate: PredicateType = predicate_raw
        else:
            # Skip unexpected types
            continue

        if is_column_group:
            if not isinstance(predicate, dict):
                error("ColumnGroup partition predicate must be a dict mapping columns", errors)
                continue

            for col_name, value_raw in predicate.items():
                if not isinstance(value_raw, dict):
                    error(f"ColumnGroup partition predicate for '{col_name}' must be object", errors)
                    continue

                has_value = "partitionValue" in value_raw
                has_bounds = "csvw-safe:lowerBound" in value_raw or "csvw-safe:upperBound" in value_raw

                if not (has_value or has_bounds):
                    error(
                        f"ColumnGroup partition for '{col_name}' must define partitionValue or bounds", errors
                    )

                if has_bounds:
                    lb = value_raw.get("csvw-safe:lowerBound")
                    ub = value_raw.get("csvw-safe:upperBound")
                    if lb is None or ub is None:
                        error(
                            f"Continuous partition for '{col_name}' must define lowerBound and upperBound",
                            errors,
                        )
                    elif lb > ub:
                        error(f"Partition for '{col_name}' has lowerBound > upperBound", errors)
        else:
            # Single-column partitions
            if not isinstance(predicate, (dict, int, float, str)):
                continue  # skip invalid types

            if isinstance(predicate, dict):
                has_value = "partitionValue" in predicate
                has_bounds = "csvw-safe:lowerBound" in predicate or "csvw-safe:upperBound" in predicate
            else:
                has_value = True  # primitive values count as partitionValue
                has_bounds = False

            if not (has_value or has_bounds):
                error("Partition predicate must define partitionValue or bounds", errors)

            if has_bounds:
                lb = predicate.get("csvw-safe:lowerBound") if isinstance(predicate, dict) else None
                ub = predicate.get("csvw-safe:upperBound") if isinstance(predicate, dict) else None

                if lb is None or ub is None:
                    error("Numeric partition must define lowerBound and upperBound", errors)
                elif lb > ub:
                    error("Partition lowerBound > upperBound", errors)
                else:
                    numeric_intervals.append((lb, ub))

    # Check for overlapping numeric partitions in single-column partitions
    if not is_column_group:
        for i, (lb1, ub1) in enumerate(numeric_intervals):
            for lb2, ub2 in numeric_intervals[i + 1 :]:
                if intervals_overlap(lb1, ub1, lb2, ub2):
                    error("Overlapping numeric partitions detected", errors)


# ============================================================
# ColumnGroup-level validation
# ============================================================
def validate_column_groups(
    metadata: Dict[str, Any], columns_by_name: Dict[str, Any], errors: List[str]
) -> None:
    """Validate ColumnGroup entries."""
    groups = metadata.get("csvw-safe:additionalInformation", [])
    if not isinstance(groups, list):
        return

    for g in groups:
        if g.get("@type") != "csvw-safe:ColumnGroup":
            continue
        cols = g.get("csvw-safe:columns", [])
        if not isinstance(cols, list):
            continue
        if len(cols) < 2:
            error("ColumnGroup must reference at least two columns", errors)
        for col_name in cols:
            if col_name not in columns_by_name:
                error(f"ColumnGroup references unknown column '{col_name}'", errors)
            elif columns_by_name[col_name].get("csvw-safe:public.privacyId"):
                error(f"ColumnGroup cannot include privacyId column '{col_name}'", errors)
        partitions = g.get("csvw-safe:public.partitions")
        if isinstance(partitions, list):
            validate_partitions(g, partitions, errors)
        max_num = g.get("csvw-safe:public.maxNumPartitions")
        if isinstance(max_num, int) and isinstance(partitions, list) and len(partitions) > max_num:
            error("ColumnGroup exceeds public.maxNumPartitions", errors)


# ============================================================
# Main metadata validation
# ============================================================
def validate_metadata(metadata: Dict[str, Any]) -> List[str]:
    """Validate full CSVW-SAFE metadata."""
    errors: List[str] = []
    validate_table(metadata, errors)

    table_schema = metadata.get("csvw:tableSchema", {})
    columns = table_schema.get("columns", [])

    columns_by_name: Dict[str, Dict[str, Any]] = {}
    for col in columns:
        if isinstance(col, dict):
            name = col.get("name")
            if name:
                columns_by_name[name] = col
            validate_column(col, errors)

    validate_column_groups(metadata, columns_by_name, errors)
    return errors


# ============================================================
# CLI entry point
# ============================================================
def main() -> None:
    """
    CLI entry point for CSVW-SAFE metadata validation.

    Parses command-line arguments to locate a metadata JSON file, loads it,
    and runs full CSVW-SAFE validation. Prints validation results to stdout
    and exits with code 1 if any errors are found.
    """
    parser = argparse.ArgumentParser(description="Validate CSVW-SAFE metadata")
    parser.add_argument("metadata_file", type=str)
    args = parser.parse_args()

    metadata_path = Path(args.metadata_file)
    if not metadata_path.exists():
        print(f"File not found: {metadata_path}")
        sys.exit(1)

    with metadata_path.open("r", encoding="utf-8") as f:
        metadata = json.load(f)

    errors = validate_metadata(metadata)
    if errors:
        print(f"VALIDATION FAILED ({len(errors)} issues)")
        for e in errors:
            print(f" - {e}")
        sys.exit(1)
    else:
        print("VALIDATION SUCCESS: Metadata satisfies CSVW-SAFE specification")


if __name__ == "__main__":
    main()
