#!/usr/bin/env python3
"""
validate.py

Validate CSVW-DP metadata JSON files.

Features:
- Lightweight Python validation (types, bounds, partitions)
- Optional SHACL validation using csvw-dp-ext-constraints.ttl
"""

import json
import sys
from pathlib import Path

# Optional dependency for SHACL validation
try:
    from pyshacl import validate as shacl_validate
    from rdflib import Graph
    SHACL_AVAILABLE = True
except ImportError:
    SHACL_AVAILABLE = False

# ---------- Supported types ----------
VALID_TYPES = {"string", "boolean", "integer", "double", "dateTime"}


def validate_column(col: dict, col_idx: int):
    errors = []

    name = col.get("name")
    if not name:
        errors.append(f"Column at index {col_idx} missing 'name'")

    dtype = col.get("datatype")
    if dtype not in VALID_TYPES:
        errors.append(f"Column '{name}' has invalid datatype: {dtype}")

    nullable = col.get("dp:nullableProportion", 0)
    if not (0 <= nullable <= 1):
        errors.append(f"Column '{name}' dp:nullableProportion {nullable} not in [0,1]")

    if dtype in ("string", "boolean", "integer"):
        if "dp:publicPartitions" not in col:
            errors.append(f"Column '{name}' of type {dtype} missing dp:publicPartitions")
        else:
            partitions = col["dp:publicPartitions"]
            if not isinstance(partitions, list):
                errors.append(f"Column '{name}' dp:publicPartitions must be a list")

    if dtype in ("integer", "double"):
        for bound in ["minimum", "maximum"]:
            if bound not in col:
                errors.append(f"Column '{name}' of type {dtype} missing {bound}")
            else:
                if not isinstance(col[bound], (int, float)):
                    errors.append(f"Column '{name}' {bound} must be numeric")

    if dtype == "dateTime":
        for bound in ["minimum", "maximum"]:
            if bound not in col:
                errors.append(f"Column '{name}' of type dateTime missing {bound}")

    return errors


def validate_metadata(metadata: dict):
    errors = []

    table_schema = metadata.get("tableSchema")
    if not table_schema:
        errors.append("Missing 'tableSchema'")
        return errors

    # Check top-level DP fields
    for field in ["dp:maxContributions", "dp:maxTableLength", "dp:tableLength"]:
        if field not in table_schema:
            errors.append(f"Missing tableSchema field: {field}")

    # Validate columns
    columns = table_schema.get("columns", [])
    if not isinstance(columns, list):
        errors.append("tableSchema.columns must be a list")
        return errors

    for idx, col in enumerate(columns):
        errors.extend(validate_column(col, idx))

    return errors


def run_shacl_validation(metadata_file: Path, shacl_file: Path):
    if not SHACL_AVAILABLE:
        print("pySHACL not installed. Cannot run SHACL validation.")
        return False, "pySHACL not installed"

    data_graph = Graph()
    data_graph.parse(metadata_file, format="json-ld")

    shacl_graph = Graph()
    shacl_graph.parse(shacl_file, format="turtle")

    conforms, results_graph, results_text = shacl_validate(
        data_graph,
        shacl_graph=shacl_graph,
        inference='rdfs',
        abort_on_first=False,
        meta_shacl=False,
        debug=False
    )

    return conforms, results_text


def main():
    import argparse
    parser = argparse.ArgumentParser(description="Validate CSVW-DP metadata JSON")
    parser.add_argument("metadata_file", type=str, help="Metadata JSON file")
    parser.add_argument("--shacl", type=str, default=None, help="Optional SHACL TTL file")
    args = parser.parse_args()

    metadata_path = Path(args.metadata_file)
    if not metadata_path.exists():
        print(f"File not found: {metadata_path}")
        sys.exit(1)

    with metadata_path.open("r", encoding="utf-8") as f:
        try:
            metadata = json.load(f)
        except json.JSONDecodeError as e:
            print(f"Invalid JSON: {e}")
            sys.exit(1)

    # ---------- Python validation ----------
    errors = validate_metadata(metadata)

    if errors:
        print(f"Python validation FAILED: {len(errors)} issue(s) found")
        for e in errors:
            print(f"  - {e}")
    else:
        print("Python validation SUCCESS: Metadata is valid CSVW-DP")

    # ---------- Optional SHACL validation ----------
    if args.shacl:
        shacl_file = Path(args.shacl)
        if not shacl_file.exists():
            print(f"SHACL file not found: {shacl_file}")
            sys.exit(1)

        conforms, results_text = run_shacl_validation(metadata_path, shacl_file)
        print("\nSHACL validation results:")
        if conforms:
            print("Conforms: True ✅ Metadata satisfies SHACL constraints")
        else:
            print("Conforms: False ❌ Metadata violates SHACL constraints")
            print(results_text)


if __name__ == "__main__":
    main()
