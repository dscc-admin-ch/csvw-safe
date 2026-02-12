#!/usr/bin/env python3
import pandas as pd
import numpy as np
from pathlib import Path
import argparse
import sys

# ----------------------------
# Utility: Infer simple CSVW-style datatype
# ----------------------------
def infer_dtype(series: pd.Series) -> str:
    if pd.api.types.is_bool_dtype(series):
        return "boolean"
    elif pd.api.types.is_integer_dtype(series):
        return "integer"
    elif pd.api.types.is_float_dtype(series):
        return "double"
    elif pd.api.types.is_datetime64_any_dtype(series):
        return "dateTime"
    else:
        return "string"

# ----------------------------
# Structural comparison
# ----------------------------
def assert_same_structure(csv1_path: Path, csv2_path: Path, check_categories: bool = True):
    df1 = pd.read_csv(csv1_path, parse_dates=True)
    df2 = pd.read_csv(csv2_path, parse_dates=True)

    # ----------------------------
    # Columns
    # ----------------------------
    if list(df1.columns) != list(df2.columns):
        raise AssertionError(f"Column names/order differ:\nOriginal: {list(df1.columns)}\nDummy:    {list(df2.columns)}")

    # ----------------------------
    # Data types
    # ----------------------------
    for col in df1.columns:
        dtype1 = infer_dtype(df1[col])
        dtype2 = infer_dtype(df2[col])
        if dtype1 != dtype2:
            raise AssertionError(f"Column '{col}' dtype mismatch: original={dtype1}, dummy={dtype2}")

    # ----------------------------
    # Nullability
    # ----------------------------
    for col in df1.columns:
        required1 = df1[col].notna().all()
        required2 = df2[col].notna().all()
        if required1 != required2:
            raise AssertionError(f"Column '{col}' nullability mismatch: original required={required1}, dummy required={required2}")

    # ----------------------------
    # Categorical values (optional)
    # ----------------------------
    if check_categories:
        for col in df1.columns:
            dtype = infer_dtype(df1[col])
            if dtype in ("string", "integer", "boolean"):
                vals1 = set(df1[col].dropna().unique())
                vals2 = set(df2[col].dropna().unique())
                if not vals2.issubset(vals1):
                    raise AssertionError(f"Column '{col}' dummy values {vals2} are not subset of original {vals1}")

    print(f"✅ Structure check passed: {len(df1.columns)} columns match, datatypes compatible, nullability compatible.")


# ----------------------------
# CLI
# ----------------------------
def main():
    parser = argparse.ArgumentParser(description="Assert that two CSVs have the same structure")
    parser.add_argument("original_csv", type=str, help="Original CSV file")
    parser.add_argument("dummy_csv", type=str, help="Dummy CSV file")
    parser.add_argument("--no-categories", action="store_true", help="Skip checking categorical values")
    args = parser.parse_args()

    try:
        assert_same_structure(Path(args.original_csv), Path(args.dummy_csv), check_categories=not args.no_categories)
    except AssertionError as e:
        print(f"❌ Structure mismatch: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"ERROR: {e}")
        sys.exit(2)


if __name__ == "__main__":
    main()