import argparse
import json
from pathlib import Path
from itertools import combinations, product
from typing import Tuple, List

import numpy as np
import pandas as pd
import pandas.api.types as ptypes


# ============================================================
# Utilities
# ============================================================
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
# Column level
# ============================================================
def get_continuous_bounds(series):
    non_null = series.dropna()
    if non_null.empty:
        return None, None

    value_min = non_null.min()
    value_max = non_null.max()

    if pd.api.types.is_datetime64_any_dtype(series):
        return value_min.isoformat(), value_max.isoformat()
    return value_min, value_max


def identify_fixed_fields(df, column_name, threshold=1):
    grouped = df.groupby(column_name, dropna=False)

    nunique = grouped.nunique(dropna=False)
    fixed_columns = nunique.columns[(nunique <= threshold).all()].tolist()
    if len(fixed_columns) == len(df.columns) - 1:
        return []
    return fixed_columns


def identify_dependance(
    column_name, df, mapping_threshold=0.95, coverage_threshold=0.8, max_mapping_size=25
):
    """
    Detect dependencies between columns and produce csvw-safe metadata.
    """

    results = []
    s = df[column_name]

    for col in df.columns:
        if col == column_name:
            continue

        # remove rows with NA in either column
        valid = df[[column_name, col]].dropna()

        if valid.empty:
            continue

        s_valid = valid[column_name]
        o_valid = valid[col]

        # 1. Numeric dependency (bigger / smaller / monotonic)
        if ptypes.is_numeric_dtype(s_valid) and ptypes.is_numeric_dtype(o_valid):
            corr = s_valid.corr(o_valid, method="spearman")
            if abs(corr) > 0.95:
                results.append(
                    {
                        "csvw-safe:synth.dependsOn": col,
                        "csvw-safe:synth.how": "monotonic",
                        "csvw-safe:synth.correlation": round(corr, 3),
                    }
                )
                continue

            if (s_valid >= o_valid).all():
                results.append(
                    {"csvw-safe:synth.dependsOn": col, "csvw-safe:synth.how": "bigger"}
                )
                continue

            if (s_valid <= o_valid).all():
                results.append(
                    {"csvw-safe:synth.dependsOn": col, "csvw-safe:synth.how": "smaller"}
                )
                continue

        # 2. Candidate mapping (only for reasonable cardinality)
        if valid[col].nunique() > max_mapping_size:
            continue

        # build mapping
        grouped = valid.groupby(col)[column_name].agg(lambda x: list(pd.unique(x)))
        mapping = grouped.to_dict()

        if not mapping:
            continue

        # determinism check
        deterministic_ratio = sum(len(v) == 1 for v in mapping.values()) / len(mapping)

        if deterministic_ratio < mapping_threshold:
            continue

        # coverage check
        covered_rows = valid[col].isin(mapping.keys()).sum()
        coverage = covered_rows / len(df)

        if coverage < coverage_threshold:
            continue

        clean_mapping = {k: v[0] if len(v) == 1 else v for k, v in mapping.items()}

        results.append(
            {
                "csvw-safe:synth.dependsOn": col,
                "csvw-safe:synth.how": "mapping",
                "csvw-safe:synth.mapping": clean_mapping,
            }
        )

    return results


# ============================================================
# Make Partitions
# ============================================================
def make_predicate(spec, value):
    if spec["kind"] == "categorical":
        return {"partitionValue": value}
    interval = value
    lower = (
        pd.to_datetime(interval.left).isoformat()
        if spec.get("is_datetime")
        else float(interval.left)
    )
    upper = (
        pd.to_datetime(interval.right).isoformat()
        if spec.get("is_datetime")
        else float(interval.right)
    )
    return {"lowerBound": lower, "upperBound": upper}


def make_categorical_partitions(df, privacy_unit, column_name):
    return build_partitions(
        df, privacy_unit, [{"name": column_name, "kind": "categorical"}]
    )


def make_numeric_partitions(df, privacy_unit, column_name, bounds):
    return build_partitions(
        df,
        privacy_unit,
        [
            {
                "name": column_name,
                "kind": "numeric",
                "bins": bounds,
                "is_datetime": pd.api.types.is_datetime64_any_dtype(df[column_name]),
            }
        ],
    )


def get_multi_group_partitions(df, col_group, continuous_partitions, privacy_unit):
    specs = []
    for col in col_group:
        if col in continuous_partitions:
            specs.append(
                {
                    "name": col,
                    "kind": "numeric",
                    "bins": continuous_partitions[col],
                    "is_datetime": pd.api.types.is_datetime64_any_dtype(df[col]),
                }
            )
        else:
            specs.append(
                {
                    "name": col,
                    "kind": "categorical",
                    "is_datetime": pd.api.types.is_datetime64_any_dtype(df[col]),
                }
            )
    return build_partitions(df, privacy_unit, specs)


def build_partitions(df: pd.DataFrame, privacy_unit: str, column_specs: list):
    """
    Build CSVW-SAFE partitions with per-partition contribution values.

    column_specs example:
        [
            {"name": col, "kind": "categorical", "is_datetime": False},
            {"name": col, "kind": "numeric", "bins": [...], "is_datetime": False}
        ]
    """
    df_work = (
        df.copy() if any(spec["kind"] == "numeric" for spec in column_specs) else df
    )

    # Prepare grouping columns and privacy-unit influenced counts
    grouping_columns = []
    influenced_counts = {}
    for spec in column_specs:
        col = spec["name"]

        if spec["kind"] == "categorical":
            grouping_columns.append(col)
            influenced_counts[col] = df.groupby(privacy_unit)[col].nunique(dropna=True)

        elif spec["kind"] == "numeric":
            bins = (
                pd.to_datetime(spec["bins"])
                if spec.get("is_datetime", False)
                else sorted(spec["bins"])
            )
            binned_col = f"{col}__bin"
            df_work[binned_col] = pd.cut(df_work[col], bins=bins, right=False)
            grouping_columns.append(binned_col)
            binned_col = f"{col}__bin"
            influenced_counts[col] = df_work.groupby(privacy_unit)[binned_col].nunique(
                dropna=True
            )
        else:
            raise ValueError(f"Unknown column kind {spec['kind']}")

    # Group data and get partitions information
    partitions_meta = []
    for group_key, group_df in df_work.groupby(
        grouping_columns, dropna=True, observed=True
    ):
        if not isinstance(group_key, tuple):
            group_key = (group_key,)

        predicate = {
            spec["name"]: make_predicate(spec, group_key[i])
            for i, spec in enumerate(column_specs)
        }
        if len(predicate) == 1:  # single column
            predicate = next(iter(predicate.values()))

        per_privacy_unit_contrib = group_df.groupby(privacy_unit).size()
        partitions_meta.append(
            {
                "@type": "csvw-safe:Partition",
                "csvw-safe:predicate": predicate,
                "csvw-safe:bounds.maxLength": int(group_df.shape[0]),
                "csvw-safe:bounds.maxGroupsPerUnit": int(
                    per_privacy_unit_contrib.max()
                ),
                "csvw-safe:bounds.maxContributions": max(
                    int(
                        influenced_counts[spec["name"]]
                        .loc[per_privacy_unit_contrib.index]
                        .max()
                    )
                    for spec in column_specs
                ),
            }
        )

    return partitions_meta


def column_level_continuous_partition(partitions):
    keys = [
        "csvw-safe:bounds.maxLength",
        "csvw-safe:bounds.maxGroupsPerUnit",
        "csvw-safe:bounds.maxContributions",
    ]
    return {k: max(p[k] for p in partitions) for k in keys}


def keep_predicate_only(partitions):
    """
    Extract the partition keys:
    - For categorical partitions: the actual category value
    - For numeric partitions: the lower/upper bounds dict

    partitions = [
        {"csvw-safe:predicate": {"partitionValue": "Adelie"}},
        {"csvw-safe:predicate": {"partitionValue": "Gentoo"}},
        {"csvw-safe:predicate": {"lowerBound": 0.0, "upperBound": 10.0}},
    ]
    to partition_keys = [
        "Adelie", "Gentoo", {"lowerBound": 0.0, "upperBound": 10.0}
    ]
    """
    partition_keys = []

    for partition in partitions:
        predicate = partition["csvw-safe:predicate"]

        if "partitionValue" in predicate:  # categorical
            partition_keys.append(predicate["partitionValue"])
        else:  # numeric
            partition_keys.append(predicate)

    return partition_keys


def attach_partitions_to_column(
    column_meta: dict, partitions_meta: list, col_contrib_level: str
):
    """
    Attach partitions to a column and optionally lift
    partition-level contributions to column level.
    """
    if col_contrib_level == "column":
        col_contrib = column_level_continuous_partition(partitions_meta)
        column_meta.update(col_contrib)
        partitions_meta = keep_predicate_only(partitions_meta)

    column_meta["csvw-safe:public.partitions"] = partitions_meta
    column_meta["csvw-safe:public.maxNumPartitions"] = len(partitions_meta)

    return column_meta


def make_metadata_from_data(
    df: pd.DataFrame,
    privacy_unit: str,
    max_contributions: int = 2,
    continuous_partitions: dict | None = None,
    column_groups: list | None = None,
    default_contributions_level: str = "table",  # 'table', 'column', 'partition'
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
            "../../../csvw-safe-context.jsonld",  # local path for dev (TODO later)
            # "https://w3id.org/csvw-safe/context.jsonld"
        ],
        "@type": "csvw:Table",
        "csvw-safe:public.privacyUnit": privacy_unit,
        "csvw-safe:bounds.maxContributions": int(max_contributions),
        "csvw-safe:bounds.maxLength": int(len(df)),
        "csvw-safe:public.length": int(len(df)),
        "csvw:tableSchema": {"columns": []},
    }

    for column_name in df.columns:
        series = df[column_name]
        col_contrib_level = fine_contributions_level.get(
            column_name, default_contributions_level
        )

        datatype = infer_csvw_datatype(series)
        column_meta = {
            "@type": "csvw:Column",
            "name": column_name,
            "datatype": infer_csvw_datatype(series),
            "required": bool(series.isna().sum() == 0),
            "csvw-safe:public.privacyId": column_name == privacy_unit,
            "csvw-safe:synth.nullableProportion": np.ceil(series.isna().mean() * 1000)
            / 1000,
        }
        deps = identify_dependance(column_name, df, mapping_threshold=0.95)
        if deps:
            column_meta["csvw-safe:synth.dependencies"] = deps

        fixed_fields = identify_fixed_fields(df, column_name, threshold=1)
        if fixed_fields:
            column_meta["csvw-safe:synth.fixedFields"] = fixed_fields

        if datatype != "string":
            minimum, maximum = get_continuous_bounds(series)
            column_meta["minimum"] = minimum
            column_meta["maximum"] = maximum

        if col_contrib_level != "table":
            if is_categorical(series):
                partitions_meta = make_categorical_partitions(
                    df, privacy_unit, column_name
                )
                column_meta = attach_partitions_to_column(
                    column_meta, partitions_meta, col_contrib_level
                )
            else:
                if column_name in continuous_partitions:
                    bounds = sorted(continuous_partitions[column_name])
                    partitions_meta = make_numeric_partitions(
                        df, privacy_unit, column_name, bounds
                    )
                    column_meta = attach_partitions_to_column(
                        column_meta, partitions_meta, col_contrib_level
                    )

        metadata["csvw:tableSchema"]["columns"].append(column_meta)

    if column_groups:
        additional_info = []
        for col_group in column_groups:
            for col in col_group:
                col_contrib_level = fine_contributions_level.get(
                    col, default_contributions_level
                )
                assert col_contrib_level in ["column", "partition"]
            column_meta = {
                "@type": "csvw-safe:ColumnGroup",
                "csvw-safe:columns": col_group,
            }
            partitions_meta = get_multi_group_partitions(
                df, col_group, continuous_partitions, privacy_unit
            )
            column_meta = attach_partitions_to_column(
                column_meta, partitions_meta, col_contrib_level
            )
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
        help="Column defining the privacy unit (e.g., patient_id)",
    )

    parser.add_argument(
        "--output", default="metadata.json", help="Output metadata JSON file"
    )

    parser.add_argument(
        "--max-contributions",
        type=int,
        default=2,
        help="Declared bounds.maxContributions (l_infinity)",
    )

    parser.add_argument(
        "--continuous_partitions",
        type=str,
        default=None,
        help="JSON string of bounds per continuous column",
    )

    parser.add_argument(
        "--column_groups", type=str, default=None, help="JSON string of column groups"
    )

    parser.add_argument(
        "--default_contributions_level",
        type=str,
        default="table",
        choices=["table", "column", "partition"],
        help="One of 'table', 'column', 'partition'",
    )
    parser.add_argument(
        "--fine_contributions_level",
        type=str,
        default=None,
        help="JSON string with column and expected contribution level ('column' or 'partition')",
    )
    args = parser.parse_args()

    df = pd.read_csv(args.csv_file)
    for col in df.columns:
        try:
            df[col] = pd.to_datetime(df[col])
        except (ValueError, TypeError):
            pass

    continuous_partitions = (
        json.loads(args.continuous_partitions) if args.continuous_partitions else {}
    )
    column_groups = json.loads(args.column_groups) if args.column_groups else []

    metadata = make_metadata_from_data(
        df=df,
        privacy_unit=args.privacy_unit,
        max_contributions=args.max_contributions,
        continuous_partitions=continuous_partitions,
        column_groups=column_groups,
    )

    with open(args.output, "w", encoding="utf-8") as f:
        json.dump(metadata, f, indent=2)

    print(f"CSVW-SAFE metadata written to {args.output}")


if __name__ == "__main__":
    main()
