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

def is_continuous_column(series: pd.Series) -> bool:
    if pd.api.types.is_datetime64_any_dtype(series):
        return True

    if pd.api.types.is_numeric_dtype(series):
        return not is_small_categorical_integer(series)

    return False

def infer_csvw_datatype(series: pd.Series) -> str:
    if pd.api.types.is_bool_dtype(series):
        return "boolean"
    if pd.api.types.is_datetime64_any_dtype(series):
        return "dateTime"
    if pd.api.types.is_numeric_dtype(series):
        return "double"
    return "string"


def get_categorical_values(df, categorical_cols):
    """
    Compute unique values for each categorical column in a DataFrame.
    """
    return {col: df[col].dropna().unique().tolist() for col in categorical_cols}


def generate_categorical_combinations(categorical_cols, column_values):
    """
    Generate all combinations of categorical columns.
    column_values: dict mapping categorical column to its unique values
    """
    if not categorical_cols:
        return [{}]  # no categorical columns
    # Get list of values per column
    values_list = [column_values[col] for col in categorical_cols]
    combinations = []
    for combo in product(*values_list):
        combinations.append(dict(zip(categorical_cols, combo)))
    return combinations

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
    import numpy as np

    if isinstance(obj, dict):
        return {k: sanitize(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [sanitize(v) for v in obj]
    elif isinstance(obj, np.generic):
        return obj.item()
    else:
        return obj

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


def make_numeric_partitions(bounds: list, is_datetime: bool = False):
    """
    Build continuous partitions from ordered bounds.
    Example:
        [30, 40, 50, 60] -> [30,40), [40,50), [50,60]
    """

    if len(bounds) < 2:
        return []

    partitions = []

    for i in range(len(bounds) - 1):
        lower = bounds[i]
        upper = bounds[i + 1]

        if is_datetime:
            lower = pd.to_datetime(lower).isoformat()
            upper = pd.to_datetime(upper).isoformat()
        else:
            lower = float(lower)
            upper = float(upper)

        partitions.append({
            "@type": "csvw-safe:Partition",
            "csvw-safe:predicate": {
                "lowerBound": lower,
                "upperBound": upper,
                "lowerInclusive": True,
                "upperInclusive": i == len(bounds) - 2
            }
        })

    return partitions

# ----------------------------
# Privacy Unit Contributions
# ----------------------------

def compute_margins(df, privacy_unit , col):
    col_data = df[col].dropna()

    return {
        "csvw-safe:maxNumPartitions": int(df[col].nunique(dropna=True)),
        "csvw-safe:maxPartitionLength": int(col_data.value_counts().max() if len(col_data) else 0),
        "csvw-safe:maxInfluencedPartitions": int(
            df.groupby(privacy_unit)[col].nunique(dropna=True).max()
        ),
        "csvw-safe:maxPartitionContribution": int(
            df.groupby([privacy_unit, col], observed=True).size().max()
        ),
    }


# ============================================================
# Column Group
# ============================================================

def build_column_group_from_df(df, group_columns, continuous_partitions, 
                               max_groups_per_unit=3, max_contributions=1):
    """
    Create CSVW-SAFE column group using data from a DataFrame, with categorical grouping
    and pre-specified continuous partitions.

    Args:
        df: pandas DataFrame
        group_columns: list of columns in the group
        continuous_partitions: dict {col_name: list of bin edges}, continuous columns only
        max_groups_per_unit: max groups per unit
        max_contributions: max contributions per partition

    Returns:
        dict ready to insert as a CSVW-SAFE column group
    """
    # Separate categorical and continuous columns
    cat_cols = df.select_dtypes(include=['object', 'category']).columns.intersection(group_columns).tolist()
    cont_cols = df.select_dtypes(include=['number']).columns.intersection(group_columns).tolist()

    # Verify all continuous columns have specified partitions
    for col in cont_cols:
        if col not in continuous_partitions:
            if is_small_categorical_integer(df[col]) or is_small_datetime(df[col]):
                # Treat it as categorical; can derive partitions
                cont_cols.remove(col) 
                cat_cols.append(col)
            else:
                raise ValueError(f"Cannot derive partitions for continuous column '{col}' without bin info.")

    # Bin continuous columns
    df_copy = df.copy()
    for col in cont_cols:
        bins = continuous_partitions[col]
        df_copy[col+'_bin'] = pd.cut(df_copy[col], bins=bins, right=False)

    # Group by categorical + binned continuous columns
    group_by_cols = cat_cols + [c+'_bin' for c in cont_cols]
    grouped = df_copy.groupby(group_by_cols)

    partitions = []
    for name, group in grouped:
        predicate = {}
        # Handle categorical columns
        for i, col in enumerate(cat_cols):
            val = name[i] if len(cat_cols) > 1 else name
            predicate[col] = {"partitionValue": val}

        # Handle continuous columns
        for i, col in enumerate(cont_cols):
            bin_val = name[len(cat_cols)+i] if len(group_by_cols) > len(cat_cols) else name
            predicate[col] = {"lowerBound": bin_val.left, "upperBound": bin_val.right}

        partitions.append({
            "@type": "csvw-safe:Partition",
            "csvw-safe:predicate": predicate,
            "csvw-safe:bounds.maxContributions": max_contributions
        })

    column_group = {
        "@type": "csvw-safe:ColumnGroup",
        "csvw-safe:columns": group_columns,
        "csvw-safe:bounds.maxGroupsPerUnit": max_groups_per_unit,
        "csvw-safe:public.maxNumPartitions": len(partitions),
        "csvw-safe:public.partitions": partitions
    }

    return column_group


def unique_column_group_partitions(partitions, group_columns):
    """
    Deduplicate column group partitions based on their components (handles nested dicts)
    """
    seen = set()
    unique_partitions = []
    for p in partitions:
        components = make_hashable(p["components"])
        if components not in seen:
            seen.add(components)
            unique_partitions.append(p)
    return unique_partitions

# ============================================================
# Metadata Generator
# ============================================================

def make_metadata_from_data(
    df: pd.DataFrame,
    privacy_unit: str,
    max_contributions: int = 2,
    continuous_partitions: dict | None = None,
    column_groups: list | None = None
):

    if privacy_unit not in df.columns:
        raise ValueError(f"Privacy unit column '{privacy_unit}' not found.")

    if continuous_partitions is None:
        continuous_partitions = {}
    
    if column_groups is None:
        column_groups = []

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
        ## BOOLEAN
        if pd.api.types.is_bool_dtype(series):
            partitions = make_categorical_partitions(sorted(non_null.unique()))
            column_meta["csvw-safe:public.partitions"] = partitions
            column_meta["csvw-safe:public.maxNumPartitions"] = len(partitions)
            column_meta.update(compute_margins(df, privacy_unit, column_name))
        
        ## SMALL INTEGER CATEGORICAL
        elif is_small_categorical_integer(series):
            partitions = make_categorical_partitions(
                sorted(non_null.astype(int).unique())
            )
            column_meta["datatype"] = "integer"
            column_meta["csvw-safe:public.partitions"] = partitions
            column_meta["csvw-safe:public.maxNumPartitions"] = len(partitions)
            column_meta.update(compute_margins(df, privacy_unit, column_name))
        
        ## SMALL DATETIME (categorical)
        elif is_small_datetime(series):
            print("small datetime")
            partitions = make_categorical_partitions(
                sorted(non_null.astype(str).unique())
            )
            column_meta["csvw-safe:public.partitions"] = partitions
            column_meta["csvw-safe:public.maxNumPartitions"] = len(partitions)
            column_meta.update(compute_margins(df, privacy_unit, column_name))
        
        ## CONTINUOUS NUMERIC
        elif pd.api.types.is_numeric_dtype(series) and not non_null.empty:
            partitions = []

            if column_name in continuous_partitions:
                bounds = sorted(continuous_partitions[column_name])
                partitions = make_numeric_partitions(bounds, is_datetime=False)
        
            if partitions:
                column_meta["csvw-safe:public.partitions"] = partitions
                column_meta["csvw-safe:public.maxNumPartitions"] = len(partitions)
                column_meta.update(compute_margins(df, privacy_unit, column_name))
        
        ## CONTINUOUS DATETIME (large cardinality)
        elif pd.api.types.is_datetime64_any_dtype(series) and not non_null.empty:
            partitions = []

            if column_name in continuous_partitions:
                bounds = sorted(continuous_partitions[column_name])
                partitions = make_numeric_partitions(bounds, is_datetime=True)
        
            if partitions:
                column_meta["csvw-safe:public.partitions"] = partitions
                column_meta["csvw-safe:public.maxNumPartitions"] = len(partitions)
                column_meta.update(compute_margins(df, privacy_unit, column_name))
        
        # STRING CATEGORICAL
        elif not non_null.empty:
            partitions = make_categorical_partitions(
                sorted(non_null.astype(str).unique())
            )
            column_meta["csvw-safe:public.partitions"] = partitions
            column_meta["csvw-safe:public.maxNumPartitions"] = len(partitions)
            column_meta.update(compute_margins(df, privacy_unit, column_name))

        metadata["csvw:tableSchema"]["columns"].append(column_meta)

    if column_groups:
        additional_info = []
    
        for group in column_groups:
            column_group = build_column_group_from_df(df, group, continuous_partitions)
            additional_info.append(column_group)
    
        if additional_info:
            metadata["csvw-safe:additionalInformation"] = additional_info

    metadata = sanitize(metadata)

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