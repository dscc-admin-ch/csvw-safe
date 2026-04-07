"""
Convert CSVW-SAFE JSON metadata to SmartNoise SQL metadata format.

See smarntoise-sql documentation: https://docs.smartnoise.org/sql/metadata.html
"""

import argparse
import json
from typing import Any

import yaml

from csvw_safe.constants import (
    COL_LIST,
    COL_NAME,
    DATATYPE,
    MAX_CONTRIB,
    MAXIMUM,
    MINIMUM,
    NULL_PROP,
    PRIVACY_ID,
    PUBLIC_LENGTH,
    REQUIRED,
    TABLE_SCHEMA,
)
from csvw_safe.datatypes import XSD_GROUP_MAP, DataTypesGroups, to_snsql_datatype


def csvw_to_snsql_column(col_meta: dict[str, Any]) -> dict[str, Any]:
    """
    Convert a single CSVW column metadata to SmartNoise SQL column metadata.

    Parameters
    ----------
    col_meta : dict
        Dictionary representing CSVW column metadata.
        Expected keys: "name", "datatype".

    Returns
    -------
    dict
        Dictionary representing the column metadata in SmartNoise SQL format.
    """
    if DATATYPE not in col_meta:
        raise ValueError(f"Column '{col_meta.get('name', '<unknown>')}' is missing 'datatype'")

    # Determine nullable
    if REQUIRED in col_meta:
        nullable = not col_meta[REQUIRED]
    else:
        # Default fallback to nullable_proportion
        nullable = col_meta.get(NULL_PROP, 1.0) > 0.0

    xsd_datatype = col_meta[DATATYPE]
    col_dict: dict[str, Any] = {
        "name": col_meta[COL_NAME],
        "type": to_snsql_datatype(xsd_datatype),
        "nullable": nullable,
    }

    # Mark privacy unit
    # Mark privacy unit
    if col_meta.get(PRIVACY_ID, False):
        col_dict["private_id"] = True

    if XSD_GROUP_MAP[xsd_datatype] != DataTypesGroups.DATETIME:
        if PRIVACY_ID in col_meta and col_meta[PRIVACY_ID]:
            pass
        else:
            if MINIMUM in col_meta:
                col_dict["lower"] = col_meta[MINIMUM]
            if MAXIMUM in col_meta:
                col_dict["upper"] = col_meta[MAXIMUM]

    return col_dict


def csvw_to_smartnoise_sql(  # pylint: disable=too-many-locals
    csvw_meta: dict[str, Any],
    schema_name: str = "",
    table_name: str = "df",
    row_privacy: bool | None = None,
    sample_max_ids: bool | None = None,
    censor_dims: bool | None = None,
    clamp_counts: bool | None = None,
    clamp_columns: bool | None = None,
    use_dpsu: bool | None = None,
) -> dict[str, Any]:
    """
    Convert a CSVW-SAFE table metadata dictionary to SmartNoise SQL metadata.

    Parameters
    ----------
    csvw_meta : Dict[str, Any]
        The CSVW-SAFE metadata dictionary for a single table.
        Must include "columns" list and "max_contributions" (used as max_ids).
    schema_name : str, default=""
        Name of the SmartNoise schema (top-level namespace) for the table.
    table_name : str, default="df"
        Name of the table in SmartNoise metadata.
    row_privacy : bool, default=False
        Whether to enable row-level privacy for the table.
    sample_max_ids : bool, default=True
        If True, skips reservoir sampling when users appear at most max_ids times.
    censor_dims : bool, default=True
        If True, drops GROUP BY rows that might reveal rare individuals.
    clamp_counts : bool, default=False
        If True, clamps negative differentially private counts to zero.
    clamp_columns : bool, default=True
        If True, clamps input data to column lower/upper bounds.
    use_dpsu : bool, default=False
        If True, enables Differential Private Set Union for censoring rare dimensions.

    Returns
    -------
    Dict[str, Any]
        SmartNoise SQL metadata as a nested dictionary suitable for YAML serialization.
        Structure:
        {
            "": {
                schema_name: {
                    table_name: {
                        "max_ids": ...,
                        "row_privacy": ...,
                        "sample_max_ids": ...,
                        "censor_dims": ...,
                        "clamp_counts": ...,
                        "clamp_columns": ...,
                        "use_dpsu": ...,
                        "<column_name>": {
                            "name": ...,
                            "type": ...,
                            "nullable": ...,
                            "lower": ...,
                            "upper": ...,
                            "private_id": ...
                        },
                        ...
                    }
                }
            }
        }

    Raises
    ------
    ValueError
        If "max_contributions" is missing from the CSVW metadata, since it is required
        as `max_ids`.
    """
    # Validate max_ids
    if MAX_CONTRIB not in csvw_meta:
        raise ValueError(f"CSVW metadata must include '{MAX_CONTRIB}' (max_ids for SNSQL)")
    max_ids = csvw_meta[MAX_CONTRIB]

    # Required fields only
    table_meta: dict[str, Any] = {
        "max_ids": max_ids,
    }

    if csvw_meta.get(PUBLIC_LENGTH, False):
        table_meta["rows"] = csvw_meta[PUBLIC_LENGTH]

    # Optional fields (only include if explicitly provided)
    optional_fields = {
        "row_privacy": row_privacy,
        "sample_max_ids": sample_max_ids,
        "censor_dims": censor_dims,
        "clamp_counts": clamp_counts,
        "clamp_columns": clamp_columns,
        "use_dpsu": use_dpsu,
    }
    for key, value in optional_fields.items():
        if value is not None:
            table_meta[key] = value

    # Convert columns
    has_private_id = False
    for col_meta in csvw_meta[TABLE_SCHEMA][COL_LIST]:
        col_dict = csvw_to_snsql_column(col_meta)
        table_meta[col_meta[COL_NAME]] = col_dict

        if col_dict.get("private_id", False):
            has_private_id = True

    # Override row_privacy if needed
    if has_private_id:
        if row_privacy:
            raise ValueError("Row privacy is set, but metadata specifies a private_id")
        table_meta["row_privacy"] = False

    # Wrap into schema/table hierarchy
    return {"": {schema_name: {table_name: table_meta}}}


def main() -> None:
    """
    CLI for converting CSVW-SAFE JSON metadata to SmartNoise SQL YAML metadata.

    This function reads a CSVW-SAFE JSON metadata file and converts it into SmartNoise SQL
    YAML metadata.
    All table-level options are configurable via CLI arguments, except `max_ids`, which must
    be present in the CSVW metadata (as 'max_contributions').
    Defaults and meaning are taken directly from https://docs.smartnoise.org/sql/metadata.html.

    Command-line arguments
    ----------------------
    --input : str (required)
        Path to input CSVW-SAFE JSON metadata file.
    --output : str (required)
        Path to output SmartNoise YAML metadata file.
    --schema : str (default="MySchema")
        SmartNoise schema name.
    --table : str (default="MyTable")
        SmartNoise table name.
    --row_privacy : bool (default=False)
        Treat each row as a single individual.
    --sample_max_ids : bool (default=True)
        Skip reservoir sampling if users appear at most max_ids times.
    --censor_dims : bool (default=True)
        Drop GROUP BY output rows that might reveal rare individuals.
    --clamp_counts : bool (default=False)
        Clamp negative DP counts to zero.
    --clamp_columns : bool (default=True)
        Clamp all input data to the column lower/upper bounds.
    --use_dpsu : bool (default=False)
        Use Differential Private Set Union for rare dimensions.
    """
    parser = argparse.ArgumentParser(
        description="Convert CSVW-SAFE JSON metadata to SmartNoise SQL YAML metadata."
    )
    parser.add_argument("--input", required=True, help="Input CSVW-SAFE JSON metadata file")
    parser.add_argument("--output", required=True, help="Output SmartNoise YAML metadata file")
    parser.add_argument("--schema", default="MySchema", help="SmartNoise SQL schema name")
    parser.add_argument("--table", default="MyTable", help="SmartNoise SQL table name")
    parser.add_argument("--row_privacy", type=bool, default=None, help="Enable row privacy")
    parser.add_argument(
        "--sample_max_ids", type=bool, default=None, help="Skip sampling if max_ids enforced"
    )
    parser.add_argument(
        "--censor_dims", type=bool, default=None, help="Drop GROUP BY rows revealing individuals"
    )
    parser.add_argument(
        "--clamp_counts", type=bool, default=None, help="Clamp negative counts to zero"
    )
    parser.add_argument("--clamp_columns", type=bool, default=None, help="Clamp columns to bounds")
    parser.add_argument(
        "--use_dpsu", type=bool, default=None, help="Use Differential Private Set Union"
    )

    args = parser.parse_args()

    # Load CSVW metadata
    with open(args.input, encoding="utf-8") as f:
        csvw_meta = json.load(f)

    # Call conversion function
    snsql_meta = csvw_to_smartnoise_sql(
        csvw_meta=csvw_meta,
        schema_name=args.schema,
        table_name=args.table,
        row_privacy=args.row_privacy,
        sample_max_ids=args.sample_max_ids,
        censor_dims=args.censor_dims,
        clamp_counts=args.clamp_counts,
        clamp_columns=args.clamp_columns,
        use_dpsu=args.use_dpsu,
    )

    # Write YAML
    with open(args.output, "w", encoding="utf-8") as f:
        yaml.safe_dump(snsql_meta, f)

    print(f"SmartNoise SQL metadata written to {args.output}")


if __name__ == "__main__":
    main()
