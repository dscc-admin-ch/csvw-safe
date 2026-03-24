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
from typing import TYPE_CHECKING, Any, Dict, List

import opendp.prelude as dp

from csvw_safe.constants import (
    COL_LIST,
    COL_NAME,
    EXHAUSTIVE_PARTITIONS,
    MAX_CONTRIB,
    MAX_GROUPS,
    MAX_LENGTH,
    PUBLIC_LENGTH,
)

if TYPE_CHECKING:
    from opendp.polars import Margin


def csvw_to_opendp_margins(csvw_meta: Dict[str, Any]) -> List["Margin"]:
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
    margins: List["Margin"] = []

    # Global max contribution (any non groupby query)
    if MAX_CONTRIB not in csvw_meta:
        raise ValueError(f"Missing required field '{MAX_CONTRIB}'")

    # Global max_length (for non count queries)
    global_max_length = csvw_meta.get(MAX_LENGTH)

    margin_kwargs: Dict[str, Any] = {}

    if global_max_length is not None:
        margin_kwargs["max_length"] = global_max_length

    # If PUBLIC_LENGTH → invariant lengths
    if csvw_meta.get(PUBLIC_LENGTH, False):
        margin_kwargs["invariant"] = "lengths"

    if margin_kwargs:
        margins.append(dp.polars.Margin(**margin_kwargs))  # type: ignore[attr-defined]

    # Column-level margins
    for col_meta in csvw_meta.get(COL_LIST, []):
        col_name = col_meta[COL_NAME]

        margin_kwargs = {
            "by": [col_name],
        }

        # max_length per column
        if MAX_LENGTH in col_meta:
            margin_kwargs["max_length"] = col_meta[MAX_LENGTH]

        # max_groups per column
        if MAX_GROUPS in col_meta:
            margin_kwargs["max_groups"] = col_meta[MAX_GROUPS]

        # Public keys / partitions → invariant keys
        if col_meta.get(EXHAUSTIVE_PARTITIONS):
            margin_kwargs["invariant"] = "keys"
        print(col_name)
        print(margin_kwargs)
        margins.append(dp.polars.Margin(**margin_kwargs))  # type: ignore[attr-defined]
    print(margins)
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
    with open(args.input, "r", encoding="utf-8") as f:
        csvw_meta = json.load(f)

    margins = csvw_to_opendp_margins(csvw_meta)

    # Convert Margin objects → dict (for JSON output)
    def margin_to_dict(m: dp.polars.Margin) -> Dict[str, Any]:  # type: ignore[name-defined]
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
        print(f"OpenDP margins written to {args.output}")
    else:
        print(json.dumps(margins_dict, indent=2))


if __name__ == "__main__":
    main()
