import argparse
import json
from pathlib import Path
from itertools import combinations

import numpy as np
import pandas as pd


# ============================================================
# Helper Utilities
# ============================================================

def is_small_categorical_integer(series: pd.Series, max_unique: int = 20) -> bool:
    """
    Detect whether a numeric column should be modeled as
    categorical integer partitions.
    """
    if not pd.api.types.is_numeric_dtype(series):
        return False

    non_null = series.dropna()
    if non_null.empty:
        return False

    is_integer = (non_null % 1 == 0).all()
    return is_integer and non_null.nunique() <= max_unique


def infer_csvw_datatype(series: pd.Series) -> str:
    if pd.api.types.is_bool_dtype(series):
        return "boolean"
    if pd.api.types.is_datetime64_any_dtype(series):
        return "dateTime"
    if pd.api.types.is_numeric_dtype(series):
        return "double"
    return "string"


# ============================================================
# Partition Builders
# ============================================================

def make_categorical_partitions(values):
    return [
        {
            "@type": "csvw-safe:Partition",
            "csvw-safe:predicate": {"partitionValue": v}
        }
        for v in values
    ]


def make_numeric_partitions(series: pd.Series):
    """
    Simple 2-bin heuristic partitioning for continuous variables.
    """
    min_val = float(series.min())
    max_val = float(series.max())
    midpoint = (min_val + max_val) / 2

    return [
        {
            "@type": "csvw-safe:Partition",
            "csvw-safe:predicate": {
                "lowerBound": min_val,
                "upperBound": midpoint,
                "lowerInclusive": True,
                "upperInclusive": False
            }
        },
        {
            "@type": "csvw-safe:Partition",
            "csvw-safe:predicate": {
                "lowerBound": midpoint,
                "upperBound": max_val,
                "lowerInclusive": True,
                "upperInclusive": True
            }
        }
    ]


# ============================================================
# Group Detection (Optional)
# ============================================================

def detect_column_groups(df, columns_meta):
    """
    Detect joint categorical grouping keys from observed combinations.
    """
    groups = []

    categorical_columns = [
        col["name"]
        for col in columns_meta
        if col.get("csvw-safe:public.partitions")
        and not col.get("csvw-safe:public.privacyId", False)
    ]

    for c1, c2 in combinations(categorical_columns, 2):

        joint = df[[c1, c2]].dropna()
        if joint.empty:
            continue

        partitions = []

        for _, row in joint.drop_duplicates().iterrows():
            partitions.append({
                "@type": "csvw-safe:Partition",
                "csvw-safe:predicate": {
                    "components": {
                        c1: {"partitionValue": row[c1]},
                        c2: {"partitionValue": row[c2]},
                    }
                }
            })

        groups.append({
            "@type": "csvw-safe:ColumnGroup",
            "csvw-safe:columns": [c1, c2],
            "csvw-safe:public.partitions": partitions,
            "csvw-safe:public.maxNumPartitions": len(partitions)
        })

    return groups


# ============================================================
# Metadata Generator
# ============================================================

def make_metadata_from_data(
    df: pd.DataFrame,
    privacy_unit: str,
    max_contributions: int = 2,
    auto_column_groups: bool = False,
):

    if privacy_unit not in df.columns:
        raise ValueError(f"Privacy unit column '{privacy_unit}' not found.")

    metadata = {
        "@context": [
            "http://www.w3.org/ns/csvw",
            "https://w3id.org/csvw-safe/context.jsonld"
        ],
        "@type": "csvw:Table",
        "csvw-safe:public.privacyUnit": privacy_unit,
        "csvw-safe:bounds.maxContributions": int(max_contributions),
        "csvw-safe:bounds.maxLength": int(len(df)),
        "csvw-safe:public.length": int(len(df)),
        "csvw:tableSchema": {
            "columns": []
        }
    }

    for column_name in df.columns:

        series = df[column_name]
        non_null = series.dropna()

        column_meta = {
            "@type": "csvw:Column",
            "name": column_name,
            "datatype": infer_csvw_datatype(series),
            "required": bool(series.isna().sum() == 0),
            "csvw-safe:public.privacyId": column_name == privacy_unit,
        }

        # Nullable proportion (synthetic hint)
        column_meta["csvw-safe:synth.nullableProportion"] = round(
            float(series.isna().mean()), 3
        )

        # Numeric bounds
        if pd.api.types.is_numeric_dtype(series) and not non_null.empty:
            column_meta["minimum"] = float(non_null.min())
            column_meta["maximum"] = float(non_null.max())

        # Partition logic
        if pd.api.types.is_bool_dtype(series):
            partitions = make_categorical_partitions(sorted(non_null.unique()))
            column_meta["csvw-safe:public.partitions"] = partitions
            column_meta["csvw-safe:public.maxNumPartitions"] = len(partitions)

        elif is_small_categorical_integer(series):
            partitions = make_categorical_partitions(
                sorted(non_null.astype(int).unique())
            )
            column_meta["datatype"] = "integer"
            column_meta["csvw-safe:public.partitions"] = partitions
            column_meta["csvw-safe:public.maxNumPartitions"] = len(partitions)

        elif pd.api.types.is_numeric_dtype(series) and not non_null.empty:
            partitions = make_numeric_partitions(non_null)
            column_meta["csvw-safe:public.partitions"] = partitions
            column_meta["csvw-safe:public.maxNumPartitions"] = len(partitions)

        elif not non_null.empty:
            partitions = make_categorical_partitions(
                sorted(non_null.astype(str).unique())
            )
            column_meta["csvw-safe:public.partitions"] = partitions
            column_meta["csvw-safe:public.maxNumPartitions"] = len(partitions)

        metadata["csvw:tableSchema"]["columns"].append(column_meta)

    # Optional grouping key detection
    if auto_column_groups:
        groups = detect_column_groups(
            df, metadata["csvw:tableSchema"]["columns"]
        )
        if groups:
            metadata["csvw-safe:additionalInformation"] = groups

    return metadata


# ============================================================
# CLI
# ============================================================

def main():
    parser = argparse.ArgumentParser(
        description="Generate CSVW-SAFE metadata from a CSV dataset."
    )

    parser.add_argument("csv_file", help="Path to input CSV file")

    parser.add_argument(
        "--privacy-unit",
        required=True,
        help="Column defining the privacy unit (e.g., patient_id)"
    )

    parser.add_argument(
        "--output",
        default="metadata.json",
        help="Output metadata JSON file"
    )

    parser.add_argument(
        "--max-contributions",
        type=int,
        default=2,
        help="Declared bounds.maxContributions (l_infinity)"
    )

    parser.add_argument(
        "--auto-column-groups",
        action="store_true",
        help="Enable automatic multi-column grouping detection"
    )

    args = parser.parse_args()

    df = pd.read_csv(args.csv_file)

    metadata = make_metadata_from_data(
        df=df,
        privacy_unit=args.privacy_unit,
        max_contributions=args.max_contributions,
        auto_column_groups=args.auto_column_groups,
    )

    with open(args.output, "w", encoding="utf-8") as f:
        json.dump(metadata, f, indent=2)

    print(f"CSVW-SAFE metadata written to {args.output}")


if __name__ == "__main__":
    main()