"""
Csvw_to_smartnoise.py.

Convert CSVW-SAFE JSON metadata to SmartNoise SQL metadata format.
See smarntoise-sql documentation: https://docs.smartnoise.org/sql/metadata.html
"""

import argparse
import json
from typing import Any, Dict

import yaml


def map_datatype(csvw_type: str) -> str:
    """
    Map CSVW-SAFE datatype to SmartNoise SQL type.

    Raises ValueError if csvw_type is missing or unrecognized.
    """
    if not csvw_type:
        raise ValueError("CSVW column missing 'datatype'")

    type_map = {
        "integer": "int",
        "float": "float",
        "decimal": "float",
        "double": "float",
        "string": "string",
        "boolean": "boolean",
        "date": "datetime",
        "datetime": "datetime",
        "time": "datetime",
    }
    key = csvw_type.lower()
    if key not in type_map:
        raise ValueError(f"Unrecognized CSVW datatype '{csvw_type}'")
    return type_map[key]


def csvw_to_snsql_column(col_meta: Dict[str, Any], privacy_unit: str) -> Dict[str, Any]:
    """
    Convert a single CSVW column metadata to SmartNoise SQL column metadata.

    Parameters
    ----------
    col_meta : dict
        Dictionary representing CSVW column metadata.
        Expected keys: "name", "datatype", optionally "required" or "nullable_proportion",
        optionally "privacy_id", "minimum", "maximum".
    privacy_unit : str
        Name of the column representing the privacy unit. This column will be marked as private_id.

    Returns
    -------
    dict
        Dictionary representing the column metadata in SmartNoise SQL format.
    """
    if "datatype" not in col_meta:
        raise ValueError(f"Column '{col_meta.get('name', '<unknown>')}' is missing 'datatype'")

    # Determine nullable
    if "required" in col_meta:
        nullable = not col_meta["required"]
    else:
        # Default fallback to nullable_proportion
        nullable_prop = col_meta.get("nullable_proportion", 1.0)
        nullable = nullable_prop > 0.0

    col_dict: Dict[str, Any] = {
        "name": col_meta["name"],
        "type": map_datatype(col_meta["datatype"]),
        "nullable": nullable,
    }

    # Mark privacy unit
    if col_meta.get("privacy_id", False) or col_meta["name"] == privacy_unit:
        col_dict["private_id"] = True

    # Add numeric bounds if present
    if "minimum" in col_meta:
        col_dict["lower"] = col_meta["minimum"]
    if "maximum" in col_meta:
        col_dict["upper"] = col_meta["maximum"]

    return col_dict


def csvw_to_smartnoise_sql(
    csvw_meta: Dict[str, Any],
    schema_name: str,
    table_name: str,
    privacy_unit: str,
    max_ids: int,
    row_privacy: bool,
) -> Dict[str, Any]:
    """Convert CSVW-SAFE table metadata to SmartNoise SQL table metadata."""
    table_meta: Dict[str, Any] = {"max_ids": max_ids, "row_privacy": row_privacy}

    # Convert columns
    for col_meta in csvw_meta.get("columns", []):
        col_dict = csvw_to_snsql_column(col_meta, privacy_unit)
        table_meta[col_dict["name"]] = col_dict

    return {"": {schema_name: {table_name: table_meta}}}


def main() -> None:
    """
    Command-line interface for converting CSVW-SAFE JSON metadata to SmartNoise metadata.

    This function reads a JSON file containing CSVW-SAFE metadata and translates it into the
    dictionary/YAML format required by SmartNoise SQL. It maps column datatypes, numeric bounds,
    and privacy identifiers. Any fields that cannot be inferred from the CSVW metadata (e.g.,
    `max_ids` or `row_privacy`) can be provided via CLI arguments.

    Command-line arguments
    ----------------------
    --input : str (required)
        Path to the input CSVW-SAFE JSON metadata file.
        This file is expected to contain a dictionary with the structure produced by
        your CSVW-SAFE metadata generator, including the "columns" list.

    --output : str (required)
        Path to the output SmartNoise SQL YAML metadata file.
        The YAML file will contain a nested dictionary suitable for SNSQL ingestion.

    --schema : str (default="MySchema")
        Name of the schema to use in the SmartNoise SQL metadata.
        This acts as the top-level namespace for the table.

    --table : str (default="MyTable")
        Name of the table to use in the SmartNoise SQL metadata.

    --privacy_unit : str (default="")
        Name of the column in the CSVW-SAFE metadata that identifies the privacy unit.
        This column will be marked as `private_id: True` in the SNSQL metadata.
        If left empty, no column will be marked as the private identifier.

    --max_ids : int (default=1)
        Maximum number of rows a unique user can contribute in the table.
        This sets the `max_ids` field in the SNSQL metadata.

    --row_privacy : bool (default=False)
        Indicates whether row-level privacy is enabled for the table.
        Sets the `row_privacy` field in the SNSQL metadata.
        If True, SmartNoise treats each row as a single individual.

    Behavior
    --------
    1. Loads CSVW-SAFE JSON metadata from the provided input file.
    2. Converts each column to the SNSQL format:
       - Maps CSVW types to SNSQL types ("integer" -> "int", "float" -> "float", etc.)
       - Adds numeric bounds (`minimum` and `maximum`) as `lower` and `upper`
       - Marks the privacy unit column with `private_id: True`
    3. Writes the resulting SmartNoise SQL metadata as a YAML file to the specified output path.
    4. Prints a confirmation message with the output file path.
    """
    parser = argparse.ArgumentParser(
        description="Convert CSVW-SAFE JSON metadata to SmartNoise SQL YAML metadata."
    )
    parser.add_argument("--input", required=True, help="Input CSVW-SAFE JSON metadata file")
    parser.add_argument("--output", required=True, help="Output SmartNoise YAML metadata file")
    parser.add_argument("--schema", default="schema", help="SmartNoise SQL schema name")
    parser.add_argument("--table", default="table", help="SmartNoise SQL table name")
    parser.add_argument(
        "--privacy_unit", default="", help="Column representing privacy unit (private_id)"
    )
    parser.add_argument("--max_ids", type=int, default=1, help="Maximum rows per unique user")
    parser.add_argument(
        "--row_privacy", type=bool, default=False, help="Whether to enable row privacy"
    )
    args = parser.parse_args()

    with open(args.input, "r", encoding="utf-8") as f:
        csvw_meta = json.load(f)

    snsql_meta = csvw_to_smartnoise_sql(
        csvw_meta, args.schema, args.table, args.privacy_unit, args.max_ids, args.row_privacy
    )

    with open(args.output, "w", encoding="utf-8") as f:
        yaml.safe_dump(snsql_meta, f, sort_keys=False)

    print(f"SmartNoise SQL metadata written to {args.output}")


if __name__ == "__main__":
    main()
