#!/usr/bin/env python3
"""
Utility script to verify that a generated dummy CSV preserves the structural
properties of an original CSV dataset.

The script checks:
- column names and order
- inferred CSVW-SAFE datatypes
- nullability (required vs optional columns)
- optional categorical value compatibility

It does NOT check statistical similarity, only structural compatibility.
"""

import argparse
import sys
from pathlib import Path
from typing import Set

import pandas as pd

from csvw_safe.make_metadata_from_data import infer_xmlschema_datatype


# ----------------------------
# Structural comparison
# ----------------------------
def assert_same_structure(
    df1: pd.DataFrame,
    df2: pd.DataFrame,
    check_categories: bool = True,
) -> None:
    """
    Verify that two CSV files share the same structural schema.

    The function checks column names/order, inferred datatypes,
    nullability constraints, and optionally categorical value sets.

    Parameters
    ----------
    df1 : pd.DataFrame
        Original dataframe.
    df2 : pd.DataFrame
        Dummy dataframe.
    check_categories : bool, default=True
        Whether to verify that categorical values in the dummy data
        are subsets of those in the original data.

    Raises
    ------
    AssertionError
        If any structural mismatch is detected.
    """
    # ----------------------------
    # Columns: order and names
    # ----------------------------
    if list(df1.columns) != list(df2.columns):
        raise AssertionError(
            f"Column names/order differ:\nOriginal: {list(df1.columns)}\nDummy:    {list(df2.columns)}"
        )

    # ----------------------------
    # Data types
    # ----------------------------
    for col in df1.columns:
        dtype1: str = infer_xmlschema_datatype(df1[col])
        dtype2: str = infer_xmlschema_datatype(df2[col])

        if dtype1 != dtype2:
            raise AssertionError(f"Column '{col}' dtype mismatch: original={dtype1}, dummy={dtype2}")

    # ----------------------------
    # Nullability
    # ----------------------------
    for col in df1.columns:
        required1: bool = df1[col].notna().all()
        required2: bool = df2[col].notna().all()

        if required1 != required2:
            raise AssertionError(
                f"Column '{col}' nullability mismatch: "
                f"original required={required1}, dummy required={required2}"
            )

    # ----------------------------
    # Categorical subset check
    # ----------------------------
    if check_categories:
        for col in df1.columns:
            dtype = infer_xmlschema_datatype(df1[col])

            if dtype in ("string", "integer", "boolean"):
                vals1: Set[object] = set(df1[col].dropna().unique())
                vals2: Set[object] = set(df2[col].dropna().unique())

                if not vals2.issubset(vals1):
                    raise AssertionError(
                        f"Column '{col}' dummy values {vals2} are not subset of original {vals1}"
                    )

    print(
        f"Structure check passed: {len(df1.columns)} columns match, "
        "datatypes compatible, nullability compatible."
    )


# ----------------------------
# CLI
# ----------------------------
def main() -> None:
    """
    Command-line entry point for the CSV structure validator.
    """
    parser = argparse.ArgumentParser(
        description="Assert that two CSV files match CSVW-SAFE structural properties"
    )
    parser.add_argument("original_csv", type=str, help="Original CSV file")
    parser.add_argument("dummy_csv", type=str, help="Dummy CSV file")
    parser.add_argument(
        "--no-categories",
        action="store_true",
        help="Skip categorical subset validation",
    )

    args = parser.parse_args()

    df1 = pd.read_csv(Path(args.original_csv), parse_dates=True)
    df2 = pd.read_csv(Path(args.dummy_csv), parse_dates=True)
    try:
        assert_same_structure(
            df1,
            df2,
            check_categories=not args.no_categories,
        )
    except AssertionError as e:
        print(f"Structure mismatch: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"ERROR: {e}")
        sys.exit(2)


if __name__ == "__main__":
    main()
