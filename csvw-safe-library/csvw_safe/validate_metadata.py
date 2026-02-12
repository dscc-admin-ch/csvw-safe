"""
Advanced CSVW-SAFE metadata validator.

Implements:
- Structural validation
- Recursive PartitionKey validation
- GroupingKey logic
- DP bound inheritance & override checks
- Worst-case DP consistency
"""

import json
import sys
from pathlib import Path
from typing import Dict, List, Any

VALID_TYPES = {"string", "boolean", "integer", "double", "dateTime"}


# ============================================================
# Utility helpers
# ============================================================

def is_numeric(dtype: str) -> bool:
    return dtype in ("integer", "double")


def error(msg, errors):
    errors.append(msg)


def check_unique(values, label, errors):
    if len(values) != len(set(values)):
        errors.append(f"Duplicate {label} detected")


def intervals_overlap(a1, a2, b1, b2):
    return max(a1, b1) < min(a2, b2)


def validate_no_interval_overlap(partitions, errors):
    intervals = []
    for p in partitions:
        if "csvw-safe:lowerBound" in p:
            intervals.append((p["csvw-safe:lowerBound"],
                              p["csvw-safe:upperBound"]))
    for i in range(len(intervals)):
        for j in range(i+1, len(intervals)):
            if intervals_overlap(*intervals[i], *intervals[j]):
                errors.append("Overlapping numeric partitions detected")


def validate_dp_consistency(node, errors):

    l0 = node.get("csvw-safe:maxInfluencedPartitions")
    linf = node.get("csvw-safe:maxContribution")
    l1 = node.get("csvw-safe:maxLength")

    if l0 and linf and l1:
        if l0 * linf < l1:
            errors.append("Inconsistent DP bounds: l1 exceeds l0 * l∞")

    if linf and l1 and linf > l1:
        errors.append("maxContribution > maxLength")

    if l0 and "csvw-safe:maxNumPartitions" in node:
        if l0 > node["csvw-safe:maxNumPartitions"]:
            errors.append("maxInfluencedPartitions > maxNumPartitions")

# ============================================================
# Column validation
# ============================================================

def validate_column(col: Dict[str, Any], errors: List[str]):
    name = col.get("name")
    dtype = col.get("datatype")

    if not name:
        error("Column missing 'name'", errors)
        return

    if dtype not in VALID_TYPES:
        error(f"Column '{name}' invalid datatype '{dtype}'", errors)

    # nullableProportion
    nullable = col.get("csvw-safe:nullableProportion", 0)
    if not (0 <= nullable <= 1):
        error(f"Column '{name}' nullableProportion must be in [0,1]", errors)

    # privacyId columns must not declare DP bounds
    if col.get("csvw-safe:privacyId", False):
        for dp in ["csvw-safe:maxLength",
                   "csvw-safe:maxContribution",
                   "csvw-safe:maxInfluencedPartitions"]:
            if dp in col:
                error(f"privacyId column '{name}' must not declare {dp}", errors)

    # Numeric columns must declare min/max
    if is_numeric(dtype):
        if "minimum" not in col or "maximum" not in col:
            error(f"Numeric column '{name}' must declare minimum and maximum", errors)
        else:
            if col["minimum"] > col["maximum"]:
                error(f"Column '{name}' minimum > maximum", errors)

        col_min = col.get("minimum")
        col_max = col.get("maximum")

        if col.get("csvw-safe:publicPartitions", 0):
            for p in col["csvw-safe:publicPartitions"]:
                if "csvw-safe:lowerBound" in p:
                    if p["csvw-safe:lowerBound"] < col_min or \
                       p["csvw-safe:upperBound"] > col_max:
                        error(f"Partition outside column domain in '{name}'", errors)

    # Validate partitions if present
    if "csvw-safe:publicPartitions" in col:
        partitions = col["csvw-safe:publicPartitions"]
        validate_partitions(
            col,
            partitions,
            parent_bounds=extract_dp_bounds(col),
            column_lookup={name: col},
            errors=errors
        )
        validate_no_interval_overlap(partitions, errors)

    # required implies nullableProportion == 0
    if col.get("required") is True:
        if col.get("csvw-safe:nullableProportion", 0) != 0:
            error(f"Column '{name}' is required but nullableProportion != 0", errors)
    
    # maxNumPartitions consistency
    if "csvw-safe:maxNumPartitions" in col and \
       "csvw-safe:publicPartitions" in col:
        if len(col["csvw-safe:publicPartitions"]) > col["csvw-safe:maxNumPartitions"]:
            error(f"Column '{name}' has more publicPartitions than maxNumPartitions", errors)

    validate_dp_consistency(col, errors)

# ============================================================
# Partition validation (recursive)
# ============================================================

def validate_partitions(parent,
                        partitions,
                        parent_bounds,
                        column_lookup,
                        errors):

    if not isinstance(partitions, list):
        error("publicPartitions must be a list", errors)
        return

    for p in partitions:
        validate_partition_key(
            parent,
            p,
            parent_bounds,
            column_lookup,
            errors
        )


def validate_partition_key(parent,
                           p,
                           parent_bounds,
                           column_lookup,
                           errors):

    has_value = "csvw-safe:partitionValue" in p
    has_bounds = "csvw-safe:lowerBound" in p or "csvw-safe:upperBound" in p
    has_components = "csvw-safe:components" in p

    if not (has_value or has_bounds or has_components):
        error("PartitionKey must define value OR bounds OR components", errors)

    # Numeric bounds validation
    if has_bounds:
        lb = p.get("csvw-safe:lowerBound")
        ub = p.get("csvw-safe:upperBound")

        if lb is None or ub is None:
            error("Numeric partition must define both lowerBound and upperBound", errors)
        elif lb > ub:
            error("Partition lowerBound > upperBound", errors)

    # Recursive components
    if has_components:
        comps = p["csvw-safe:components"]
        if not isinstance(comps, dict):
            error("components must be a dict", errors)
            return

        for col_name, subpart in comps.items():
            if col_name not in column_lookup:
                error(f"Partition references unknown column '{col_name}'", errors)
                continue

            validate_partition_key(
                parent,
                subpart,
                parent_bounds,
                column_lookup,
                errors
            )

    # DP override checks
    for bound in ["csvw-safe:maxLength",
                  "csvw-safe:maxContribution"]:
        if bound in p and bound in parent_bounds:
            if p[bound] > parent_bounds[bound]:
                error(f"Partition override {bound} exceeds parent bound", errors)

    # publicLength ≤ maxLength
    if "csvw-safe:publicLength" in p and "csvw-safe:maxLength" in p:
        if p["csvw-safe:publicLength"] > p["csvw-safe:maxLength"]:
            error("Partition publicLength > maxLength", errors)

# ============================================================
# ColumnGroup validation
# ============================================================

def validate_column_groups(metadata, columns_by_name, errors):

    groups = metadata.get("csvw-safe:columnGroups", [])
    if not isinstance(groups, list):
        return

    for g in groups:

        cols = g.get("csvw-safe:columns", [])
        if len(cols) < 2:
            error("ColumnGroup must contain at least two columns", errors)

        # Check columns exist
        for col_name in cols:
            if col_name not in columns_by_name:
                error(f"ColumnGroup references unknown column '{col_name}'", errors)
            else:
                if columns_by_name[col_name].get("csvw-safe:privacyId"):
                    error(f"ColumnGroup contains privacyId column '{col_name}'", errors)

        parent_bounds = extract_dp_bounds(g)

        # Validate partitions
        if "csvw-safe:publicPartitions" in g:
            validate_partitions(
                g,
                g["csvw-safe:publicPartitions"],
                parent_bounds,
                columns_by_name,
                errors
            )

        # maxNumPartitions consistency
        if "csvw-safe:maxNumPartitions" in g:
            for col_name in cols:
                if "csvw-safe:maxNumPartitions" not in columns_by_name[col_name]:
                    error("ColumnGroup declares maxNumPartitions but a column does not", errors)

        # Group bounds must not exceed column bounds
        for bound in ["csvw-safe:maxLength",
                      "csvw-safe:maxContribution",
                      "csvw-safe:maxInfluencedPartitions"]:
        
            if bound in g:
                col_bounds = []
                for col_name in cols:
                    if bound in columns_by_name[col_name]:
                        col_bounds.append(columns_by_name[col_name][bound])
        
                if col_bounds and g[bound] > min(col_bounds):
                    error(f"ColumnGroup {bound} exceeds column bound", errors)

        # Cartesion subset enforcement
        if "csvw-safe:publicPartitions" in g:
            seen = []
            for p in g["csvw-safe:publicPartitions"]:
                key_tuple = tuple(sorted(p.get("csvw-safe:components", {}).keys()))
                seen.append(key_tuple)
            check_unique(seen, "ColumnGroup partition combinations", errors)

            for p in g.get("csvw-safe:publicPartitions", []):
                comps = p.get("csvw-safe:components", {})
                if set(comps.keys()) != set(cols):
                    error("Partition components do not match ColumnGroup columns", errors)

        validate_dp_consistency(g, errors)

# ============================================================
# Table validation
# ============================================================

def validate_table(table, errors):

    if table.get("csvw-safe:maxInfluencedPartitions", 1) != 1:
        error("Table-level maxInfluencedPartitions must equal 1", errors)

    if "csvw-safe:publicLength" in table and "csvw-safe:maxLength" in table:
        if table["csvw-safe:publicLength"] > table["csvw-safe:maxLength"]:
            error("Table publicLength > maxLength", errors)

    if "csvw-safe:maxContribution" in table and "csvw-safe:maxLength" in table:
        if table["csvw-safe:maxContribution"] > table["csvw-safe:maxLength"]:
            error("Table maxContribution > maxLength", errors)

    validate_dp_consistency(table, errors)

# ============================================================
# DP Bound extraction
# ============================================================

def extract_dp_bounds(node):
    bounds = {}
    for key in ["csvw-safe:maxLength",
                "csvw-safe:maxContribution",
                "csvw-safe:maxInfluencedPartitions"]:
        if key in node:
            bounds[key] = node[key]
    return bounds


# ============================================================
# Main metadata validation
# ============================================================

def validate_metadata(metadata):

    errors = []

    table = metadata
    validate_table(table, errors)

    table_schema = metadata.get("tableSchema", {})
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
    parser = argparse.ArgumentParser(description="Validate CSVW-SAFE metadata JSON")
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
        print("VALIDATION SUCCESS: Metadata satisfies CSVW-SAFE rules")


if __name__ == "__main__":
    main()