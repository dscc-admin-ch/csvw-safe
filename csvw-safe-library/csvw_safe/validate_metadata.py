import json
import sys
from pathlib import Path
from typing import Dict, List, Any

VALID_TYPES = {"string", "boolean", "integer", "double", "dateTime"}


# ============================================================
# Utilities
# ============================================================


def error(msg, errors):
    errors.append(msg)


def is_numeric(dtype: str) -> bool:
    return dtype in ("integer", "double")


def intervals_overlap(a1, a2, b1, b2):
    return max(a1, b1) < min(a2, b2)


# ============================================================
# Table Validation
# ============================================================


def validate_table(metadata: Dict[str, Any], errors: List[str]):

    # Privacy unit required
    if "csvw-safe:public.privacyUnit" not in metadata:
        error("Missing csvw-safe:public.privacyUnit at table level", errors)

    # Bounds
    bounds_max_len = metadata.get("csvw-safe:bounds.maxLength")
    public_len = metadata.get("csvw-safe:public.length")

    if bounds_max_len is not None and public_len is not None:
        if public_len > bounds_max_len:
            error("public.length exceeds bounds.maxLength", errors)

    if "csvw-safe:bounds.maxContributions" in metadata:
        if metadata["csvw-safe:bounds.maxContributions"] < 1:
            error("bounds.maxContributions must be >= 1", errors)


# ============================================================
# Column Validation
# ============================================================


def validate_column(col: Dict[str, Any], errors: List[str]):

    name = col.get("name")
    dtype = col.get("datatype")

    if not name:
        error("Column missing 'name'", errors)
        return

    if dtype not in VALID_TYPES:
        error(f"Column '{name}' invalid datatype '{dtype}'", errors)

    # privacyId must be boolean
    if "csvw-safe:public.privacyId" in col:
        if not isinstance(col["csvw-safe:public.privacyId"], bool):
            error(f"Column '{name}' privacyId must be boolean", errors)

    # Required consistency
    if col.get("required") and col.get("csvw-safe:synth.nullableProportion", 0) != 0:
        error(f"Column '{name}' is required but nullableProportion != 0", errors)

    # Numeric domain validation
    if is_numeric(dtype):
        if "minimum" not in col or "maximum" not in col:
            error(f"Numeric column '{name}' must declare minimum and maximum", errors)
        else:
            if col["minimum"] > col["maximum"]:
                error(f"Column '{name}' minimum > maximum", errors)

    # Partition validation
    if "csvw-safe:public.partitions" in col:
        partitions = col["csvw-safe:public.partitions"]

        if not isinstance(partitions, list):
            error(f"Column '{name}' public.partitions must be list", errors)
        else:
            validate_partitions(col, partitions, errors)

        # maxNumPartitions consistency
        if "csvw-safe:public.maxNumPartitions" in col:
            if len(partitions) > col["csvw-safe:public.maxNumPartitions"]:
                error(
                    f"Column '{name}' exceeds declared public.maxNumPartitions", errors
                )


# ============================================================
# Partition Validation
# ============================================================


def validate_partitions(parent, partitions, errors):

    intervals = []
    is_column_group = parent.get("@type") == "csvw-safe:ColumnGroup"

    for p in partitions:

        if p.get("@type") != "csvw-safe:Partition":
            error("Partition must declare @type csvw-safe:Partition", errors)

        predicate = p.get("csvw-safe:predicate")

        if not isinstance(predicate, dict):
            error("Partition missing predicate object", errors)
            continue

        if not predicate:
            error("Partition predicate cannot be empty", errors)
            continue

        # ============================================================
        # COLUMN GROUP CASE (multiple columns inside predicate)
        # ============================================================
        if is_column_group:

            for col_name, value in predicate.items():

                if not isinstance(value, dict):
                    error(
                        f"ColumnGroup partition predicate for '{col_name}' must be object",
                        errors,
                    )
                    continue

                has_value = "partitionValue" in value
                has_bounds = "lowerBound" in value or "upperBound" in value

                if not (has_value or has_bounds):
                    error(
                        f"ColumnGroup partition for '{col_name}' must define partitionValue or bounds",
                        errors,
                    )

                if has_bounds:
                    lb = value.get("lowerBound")
                    ub = value.get("upperBound")

                    if lb is None or ub is None:
                        error(
                            f"Continuous partition for '{col_name}' must define lowerBound and upperBound",
                            errors,
                        )
                    elif lb > ub:
                        error(
                            f"Partition for '{col_name}' has lowerBound > upperBound",
                            errors,
                        )

        # ============================================================
        # SINGLE COLUMN CASE
        # ============================================================
        else:
            has_value = "partitionValue" in predicate
            has_bounds = "lowerBound" in predicate or "upperBound" in predicate

            if not (has_value or has_bounds):
                error(
                    "Partition predicate must define partitionValue or bounds",
                    errors,
                )

            if has_bounds:
                lb = predicate.get("lowerBound")
                ub = predicate.get("upperBound")

                if lb is None or ub is None:
                    error(
                        "Numeric partition must define lowerBound and upperBound",
                        errors,
                    )
                elif lb > ub:
                    error("Partition lowerBound > upperBound", errors)
                else:
                    intervals.append((lb, ub))

    # ============================================================
    # Overlap detection ONLY for single numeric column partitions
    # ============================================================
    if not is_column_group:
        for i in range(len(intervals)):
            for j in range(i + 1, len(intervals)):
                if intervals_overlap(*intervals[i], *intervals[j]):
                    error("Overlapping numeric partitions detected", errors)


# ============================================================
# ColumnGroup Validation
# ============================================================


def validate_column_groups(metadata, columns_by_name, errors):

    groups = metadata.get("csvw-safe:additionalInformation", [])

    if not isinstance(groups, list):
        return

    for g in groups:

        if g.get("@type") != "csvw-safe:ColumnGroup":
            continue

        cols = g.get("csvw-safe:columns", [])

        if len(cols) < 2:
            error("ColumnGroup must reference at least two columns", errors)

        for col_name in cols:
            if col_name not in columns_by_name:
                error(f"ColumnGroup references unknown column '{col_name}'", errors)
            elif columns_by_name[col_name].get("csvw-safe:public.privacyId"):
                error(
                    f"ColumnGroup cannot include privacyId column '{col_name}'", errors
                )

        # Partition validation
        if "csvw-safe:public.partitions" in g:
            validate_partitions(g, g["csvw-safe:public.partitions"], errors)

        # maxNumPartitions consistency
        if "csvw-safe:public.maxNumPartitions" in g:
            if (
                len(g.get("csvw-safe:public.partitions", []))
                > g["csvw-safe:public.maxNumPartitions"]
            ):
                error("ColumnGroup exceeds public.maxNumPartitions", errors)


# ============================================================
# Main Validation Entry
# ============================================================


def validate_metadata(metadata):

    errors = []

    validate_table(metadata, errors)

    table_schema = metadata.get("csvw:tableSchema", {})
    columns = table_schema.get("columns", [])

    columns_by_name = {}

    for col in columns:
        columns_by_name[col.get("name")] = col
        validate_column(col, errors)

    validate_column_groups(metadata, columns_by_name, errors)

    return errors


# ============================================================
# CLI
# ============================================================


def main():
    import argparse

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
