import argparse
import json
from pathlib import Path
from itertools import combinations, product
from typing import Tuple, List

import numpy as np
import pandas as pd


# ============================================================
# Utilities
# ============================================================
def make_hashable(obj):
    """
    Recursively convert dicts/lists into tuples for hashing.
    """
    if isinstance(obj, dict):
        return tuple(sorted((k, make_hashable(v)) for k, v in obj.items()))
    elif isinstance(obj, list):
        return tuple(make_hashable(e) for e in obj)
    else:
        return obj


def sanitize(obj):
    if isinstance(obj, dict):
        return {k: sanitize(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [sanitize(v) for v in obj]
    elif isinstance(obj, np.generic):
        return obj.item()
    else:
        return obj



# ============================================================
# Types
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


def is_small_datetime(series: pd.Series, max_unique: int = 20) -> bool:
    if not pd.api.types.is_datetime64_any_dtype(series):
        return False
    return series.dropna().nunique() <= max_unique


def infer_csvw_datatype(series: pd.Series) -> str:
    if pd.api.types.is_bool_dtype(series):
        return "boolean"
    if pd.api.types.is_datetime64_any_dtype(series):
        return "dateTime"
    if pd.api.types.is_numeric_dtype(series):
        return "double"
    return "string"


def is_categorical(series):
    if pd.api.types.is_bool_dtype(series):
        return True

    if is_small_categorical_integer(series):
        return True

    if is_small_datetime(series):
        return True

    return not (
        pd.api.types.is_numeric_dtype(series)
        or pd.api.types.is_datetime64_any_dtype(series)
    )

# ============================================================
# Make Partitions
# ============================================================
def make_categorical_partitions(df, privacy_unit, column_name):
    return build_partitions(
        df,
        privacy_unit,
        [{"name": column_name, "kind": "categorical"}]
    )

def make_numeric_partitions(df, privacy_unit, column_name, bounds):
    return build_partitions(
        df,
        privacy_unit,
        [{
            "name": column_name,
            "kind": "numeric",
            "bins": bounds
        }]
    )

def get_multi_group_partitions(df, col_group, continuous_partitions, privacy_unit):
    specs = []
    for col in col_group:
        if col in continuous_partitions:
            specs.append({
                "name": col,
                "kind": "numeric",
                "bins": continuous_partitions[col]
            })
        else:
            specs.append({
                "name": col,
                "kind": "categorical"
            })
    return build_partitions(df, privacy_unit, specs)

# ============================================================
# Partitions
# ============================================================

def build_partitions(
    df: pd.DataFrame,
    privacy_unit: str,
    column_specs: list,
):
    """
    Unified partition builder.

    column_specs:
        [
            {"name": col, "kind": "categorical"},
            {"name": col, "kind": "numeric", "bins": [...]}
        ]
    """

    df_copy = df.copy()
    grouping_columns = []
    single_column = len(column_specs) == 1

    # -------------------------------------------------
    # Prepare grouping columns (bin numeric columns)
    # -------------------------------------------------
    for spec in column_specs:
        col = spec["name"]

        if spec["kind"] == "categorical":
            grouping_columns.append(col)
        elif spec["kind"] == "numeric":
            if pd.api.types.is_datetime64_any_dtype(df_copy[col]):
                bins = pd.to_datetime(spec["bins"])
            else:
                bins = sorted(spec["bins"])
            binned_col = f"{col}__bin"
            df_copy[binned_col] = pd.cut(df_copy[col], bins=bins, right=False)
            grouping_columns.append(binned_col)
        else:
            raise ValueError(f"Unknown column kind {spec['kind']}")

    # -------------------------------------------------
    # Group
    # -------------------------------------------------
    grouped = df_copy.groupby(grouping_columns, dropna=True, observed=True)

    partitions_meta = []

    for group_key, group_df in grouped:
        # Normalize pandas group_key
        if not isinstance(group_key, tuple):
            group_key_tuple = (group_key,)
        else:
            group_key_tuple = group_key
            
        if single_column:
            spec = column_specs[0]
            col = spec["name"]
            value = group_key_tuple[0]

            # ---- Flat predicate style ----
            if spec["kind"] == "categorical":
                predicate = {"partitionValue": value}

            else:  # numeric
                interval = value
                if pd.api.types.is_datetime64_any_dtype(df[spec["name"]]):
                    lower = pd.to_datetime(interval.left).isoformat()
                    upper = pd.to_datetime(interval.right).isoformat()
                else:
                    lower = float(interval.left)
                    upper = float(interval.right)
            
                predicate = {
                    "lowerBound": lower,
                    "upperBound": upper,
                }

            # ---- Preserve original single-column contributions ----
            if spec["kind"] == "categorical":
                max_influenced = int(
                    df.groupby(privacy_unit)[col]
                      .nunique(dropna=True)
                      .max()
                )
                max_partition_contribution = int(
                    df.groupby([privacy_unit, col], observed=True)
                      .size()
                      .max()
                )
            else:
                max_influenced = int(
                    group_df.groupby(privacy_unit)
                            .size()
                            .groupby(level=0)
                            .size()
                            .max()
                )
                max_partition_contribution = int(
                    group_df.groupby(privacy_unit, observed=True)
                            .size()
                            .max()
                )

        else:
            # ---- Nested predicate style ----
            predicate = {}

            if not isinstance(group_key, tuple):
                group_key = (group_key,)

            for i, spec in enumerate(column_specs):
                col = spec["name"]
                value = group_key[i]

                if spec["kind"] == "categorical":
                    predicate[col] = {"partitionValue": value}
                else:
                    predicate[col] = {
                        "lowerBound": float(value.left),
                        "upperBound": float(value.right),
                    }

            max_influenced = int(
                group_df.groupby(privacy_unit)
                        .size()
                        .groupby(level=0)
                        .size()
                        .max()
            )

            max_partition_contribution = int(
                group_df.groupby(privacy_unit, observed=True)
                        .size()
                        .max()
            )

        partitions_meta.append({
            "@type": "csvw-safe:Partition",
            "csvw-safe:predicate": predicate,
            "csvw-safe:maxPartitionLength": group_df.shape[0],
            "csvw-safe:maxInfluencedPartitions": max_influenced,
            "csvw-safe:maxPartitionContribution": max_partition_contribution,
        })

    return partitions_meta

# ============================================================
# Partitions on Column dependign of level
# ============================================================

def column_level_continuous_partition(partitions):
    max_partition_length = 0
    max_influenced_partitions = 0
    max_partition_contribution = 0
    for partition in partitions:
        if partition["csvw-safe:maxPartitionLength"] >= max_partition_length:
            max_partition_length = partition["csvw-safe:maxPartitionLength"]
        if partition["csvw-safe:maxInfluencedPartitions"] >= max_influenced_partitions:
            max_influenced_partitions = partition["csvw-safe:maxInfluencedPartitions"]
        if partition["csvw-safe:maxPartitionContribution"] >= max_partition_contribution:
            max_partition_contribution = partition["csvw-safe:maxPartitionContribution"]
    
    column_meta = {
        "csvw-safe:maxPartitionLength": max_partition_length,
        "csvw-safe:maxInfluencedPartitions": max_influenced_partitions,
        "csvw-safe:maxPartitionContribution": max_partition_contribution
    }

    return column_meta


def remove_contrib_from_partitions(partitions):
    keys_to_remove = {
        "csvw-safe:maxPartitionLength",
        "csvw-safe:maxInfluencedPartitions",
        "csvw-safe:maxPartitionContribution",
    }

    return [
        {k: v for k, v in p.items() if k not in keys_to_remove}
        for p in partitions
    ]


def attach_partitions_to_column(
    column_meta: dict,
    partitions_meta: list,
    column_contributions_level: str,
):
    """
    Attach partitions to a column and optionally lift
    partition-level contributions to column level.
    """

    if column_contributions_level == "column":
        col_contrib = column_level_continuous_partition(partitions_meta)
        column_meta.update(col_contrib)
        partitions_meta = remove_contrib_from_partitions(partitions_meta)

    column_meta["csvw-safe:public.partitions"] = partitions_meta
    column_meta["csvw-safe:public.maxNumPartitions"] = len(partitions_meta)

    return column_meta

# ============================================================
# Metadata Generator
# ============================================================

def make_metadata_from_data(
    df: pd.DataFrame,
    privacy_unit: str,
    max_contributions: int = 2,
    continuous_partitions: dict | None = None,
    column_groups: list | None = None,
    default_contributions_level: str = 'table', # 'table', 'column', 'partition'
    fine_contributions_level: dict | None = None,
):

    if privacy_unit not in df.columns:
        raise ValueError(f"Privacy unit column '{privacy_unit}' not found.")

    if continuous_partitions is None:
        continuous_partitions = {}
    
    if column_groups is None:
        column_groups = []

    if fine_contributions_level is None:
        fine_contributions_level = {}

    metadata = {
        "@context": [
            "http://www.w3.org/ns/csvw",
            "../../../csvw-safe-context.jsonld" # local path for dev (TODO later)
            #"https://w3id.org/csvw-safe/context.jsonld"
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
        column_contributions_level = fine_contributions_level.get(
            column_name,
            default_contributions_level
        )

        column_meta = {
            "@type": "csvw:Column",
            "name": column_name,
            "datatype": infer_csvw_datatype(series),
            "required": bool(series.isna().sum() == 0),
            "csvw-safe:public.privacyId": column_name == privacy_unit,
            "csvw-safe:synth.nullableProportion": np.ceil(series.isna().mean() * 1000) / 1000
        }

        if is_categorical(series):
            partitions_meta = make_categorical_partitions(df, privacy_unit, column_name)
            column_meta = attach_partitions_to_column(column_meta, partitions_meta, column_contributions_level)
        else:
            value_min = series.dropna().min()
            value_max = series.dropna().max()
            
            if pd.api.types.is_datetime64_any_dtype(series):
                column_meta["minimum"] = value_min.isoformat()
                column_meta["maximum"] = value_max.isoformat()
            else:
                column_meta["minimum"] = float(value_min)
                column_meta["maximum"] = float(value_max)

            if column_name in continuous_partitions:
                assert column_contributions_level in ["column", "partition"] # no point otherwise
                bounds = sorted(continuous_partitions[column_name])
                partitions_meta = make_numeric_partitions(df, privacy_unit, column_name, bounds)
                column_meta = attach_partitions_to_column(column_meta, partitions_meta, column_contributions_level)

            metadata["csvw:tableSchema"]["columns"].append(column_meta)


    if column_groups:
        additional_info = []
        for col_group in column_groups:
            column_meta = {
                "@type": "csvw-safe:ColumnGroup",
                "csvw-safe:columns": column_groups
            }
            partitions_meta = get_multi_group_partitions(df, col_group, continuous_partitions, privacy_unit)
            column_meta = attach_partitions_to_column(column_meta, partitions_meta, column_contributions_level)
            additional_info.append(column_meta)
    
        metadata["csvw-safe:additionalInformation"] = additional_info

    return sanitize(metadata)


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
        "--continuous_partitions",
        type=str,
        default=None,
        help="JSON string of bounds per continuous column"
    )

    parser.add_argument(
        "--column_groups",
        type=str,
        default=None,
        help="JSON string of column groups"
    )
    
    parser.add_argument(
        "--default_contributions_level",
        type=str,
        default='table',
        choices=['table', 'column', 'partition'],
        help="One of 'table', 'column', 'partition'"
    )
    parser.add_argument(
        "--fine_contributions_level",
        type=str,
        default=None,
        help="JSON string with column and expected contribution level ('column' or 'partition')"
    )
    args = parser.parse_args()

    df = pd.read_csv(args.csv_file)

    continuous_partitions = (
        json.loads(args.continuous_partitions)
        if args.continuous_partitions
        else {}
    )
    column_groups = (
        json.loads(args.column_groups)
        if args.column_groups
        else []
    )

    metadata = make_metadata_from_data(
        df=df,
        privacy_unit=args.privacy_unit,
        max_contributions=args.max_contributions,
        continuous_partitions=continuous_partitions,
        column_groups=column_groups
    )

    with open(args.output, "w", encoding="utf-8") as f:
        json.dump(metadata, f, indent=2)

    print(f"CSVW-SAFE metadata written to {args.output}")


if __name__ == "__main__":
    main()