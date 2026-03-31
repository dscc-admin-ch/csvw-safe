"""
Convert CSVW-SAFE JSON metadata into OpenDP margin descriptors.

This module provides:
- A function to translate CSVW-SAFE differential privacy metadata into
  OpenDP `dp.polars.Margin` objects.
- A CLI for generating margin specifications from a JSON metadata file.

The resulting margins can be used in an OpenDP context, for example:

    dp.Context.compositor(
        data=...,
        privacy_unit=dp.unit_of(contributions=...),
        privacy_loss=dp.loss_of(epsilon=...),
        margins=[...],
    )
"""

import argparse
import json
from typing import Any

import opendp.prelude as dp
from opendp.extras.polars import Margin

from csvw_safe.constants import (
    ADD_INFO,
    COL_LIST,
    COL_NAME,
    COLUMNS_IN_GROUP,
    EXHAUSTIVE_PARTITIONS,
    MAX_GROUPS,
    MAX_LENGTH,
    MAX_NUM_PARTITIONS,
    PUBLIC_LENGTH,
)


def get_margins(col_meta: dict[str, Any], by: list[str]) -> dict[str, Any]:
    """
    Build margin keyword arguments for a given column or column group.

    Parameters
    ----------
    col_meta : Dict[str, Any]
        Metadata describing a column or group of columns, including
        differential privacy constraints (e.g., max_length, max_groups).
    by : List[str]
        Column name(s) to group by when defining the margin.

    Returns
    -------
    Dict[str, Any]
        Dictionary of keyword arguments suitable for constructing an
        OpenDP Margin object.
    """
    margin_kwargs: dict[str, Any] = {"by": by}

    # max_length per column
    if MAX_LENGTH in col_meta:
        margin_kwargs["max_length"] = col_meta[MAX_LENGTH]

    # max_groups per column
    if MAX_GROUPS in col_meta:
        margin_kwargs["max_groups"] = col_meta[MAX_GROUPS]
    elif MAX_NUM_PARTITIONS in col_meta:
        margin_kwargs["max_groups"] = col_meta[MAX_NUM_PARTITIONS]

    # Exhaustive partitions --> invariant keys
    if col_meta.get(EXHAUSTIVE_PARTITIONS):
        margin_kwargs["invariant"] = "keys"

    if col_meta.get(PUBLIC_LENGTH):
        margin_kwargs["invariant"] = "lengths"

    return margin_kwargs


def csvw_to_opendp_margins(csvw_meta: dict[str, Any]) -> list["Margin"]:
    """
    Convert CSVW-SAFE metadata to a list of OpenDP Margin objects.

    Parameters
    ----------
    csvw_meta : Dict[str, Any]
        CSVW-SAFE metadata dictionary.

    Returns
    -------
    List["Margin"]
        List of OpenDP margin descriptors.

    Raises
    ------
    ValueError
        If required metadata (e.g., max_contributions) is missing.
    """
    margins: list[Margin] = []

    # Table-level margins: non groupby queries (by=[], max_length=10, ...)
    margin_kwargs: dict[str, Any] = {}

    # Max length (for non count queries)
    if csvw_meta.get(MAX_LENGTH, False):
        margin_kwargs["max_length"] = csvw_meta[MAX_LENGTH]

    # If length is public --> invariant lengths
    if csvw_meta.get(PUBLIC_LENGTH, False):
        margin_kwargs["invariant"] = "lengths"

    if margin_kwargs:
        margins.append(Margin(**margin_kwargs))

    # Column-level margins: groupby queries (by=['col_name'], max_length=100, ...)
    for col_meta in csvw_meta.get(COL_LIST, []):
        margin_kwargs = get_margins(col_meta, by=[col_meta[COL_NAME]])
        margins.append(Margin(**margin_kwargs))

    # Multi-columns-level margins: groupby queries (by=['col_1', 'col_2'], max_length=100, ...)
    for cols_meta in csvw_meta.get(ADD_INFO, []):
        margin_kwargs = get_margins(cols_meta, by=cols_meta[COLUMNS_IN_GROUP])
        margins.append(Margin(**margin_kwargs))

    return margins


def main() -> None:
    """
    CLI for converting CSVW-SAFE metadata to OpenDP margins.

    Reads a CSVW-SAFE JSON file and produces a Python representation
    of OpenDP margins (printed or saved).

    Command-line arguments
    ----------------------
    --input : str (required)
        Path to CSVW-SAFE JSON metadata file.

    --output : str (optional)
        Path to output JSON file containing margin descriptions.
        If not provided, prints to stdout.

    Behavior
    --------
    - Parses CSVW-SAFE metadata
    - Converts to OpenDP Margin objects
    - Serializes margins into JSON-friendly format
    """
    parser = argparse.ArgumentParser(description="Convert CSVW-SAFE metadata to OpenDP margins.")
    parser.add_argument("--input", required=True, help="Input CSVW-SAFE JSON file")
    parser.add_argument("--output", help="Optional output JSON file")

    args = parser.parse_args()

    # Load metadata
    with open(args.input, encoding="utf-8") as f:
        csvw_meta = json.load(f)

    margins = csvw_to_opendp_margins(csvw_meta)

    # Convert Margin objects → dict (for JSON output)
    def margin_to_dict(m: dp.polars.Margin) -> dict[str, Any]:  # type: ignore[name-defined]
        return {
            "by": getattr(m, "by", []),
            "max_length": getattr(m, "max_length", None),
            "max_groups": getattr(m, "max_groups", None),
            "invariant": getattr(m, "invariant", None),
        }

    margins_dict = [margin_to_dict(m) for m in margins]

    if args.output:
        with open(args.output, "w", encoding="utf-8") as f:
            json.dump(margins_dict, f, indent=2)
        print(f"opendp margins written to {args.output}")
    else:
        print(json.dumps(margins_dict, indent=2))


if __name__ == "__main__":
    main()
