import argparse
import json
from pathlib import Path
import numpy as np
import pandas as pd
from itertools import combinations


# ----------------------------
# Helpers
# ----------------------------

def is_categorical_int(col, max_unique=20):
    if not pd.api.types.is_numeric_dtype(col):
        return False
    non_null = col.dropna()
    if len(non_null) == 0:
        return False
    is_int = (non_null % 1 == 0).all()
    return is_int and non_null.nunique() <= max_unique


def csvw_dtype(col):
    if pd.api.types.is_bool_dtype(col):
        return "boolean"
    if pd.api.types.is_datetime64_any_dtype(col):
        return "dateTime"
    if pd.api.types.is_numeric_dtype(col):
        return "double"
    return "string"


# ----------------------------
# DP Margins
# ----------------------------

def compute_margins(df, individual_col, col):
    col_data = df[col].dropna()

    return {
        "csvw-safe:maxNumPartitions": int(df[col].nunique(dropna=True)),
        "csvw-safe:maxPartitionLength": int(col_data.value_counts().max() if len(col_data) else 0),
        "csvw-safe:maxInfluencedPartitions": int(
            df.groupby(individual_col)[col].nunique(dropna=True).max()
        ),
        "csvw-safe:maxPartitionContribution": int(
            df.groupby([individual_col, col], observed=True).size().max()
        ),
    }


# ----------------------------
# Partition Key Detection
# ----------------------------

def detect_partition_keys(columns_meta):
    for col in columns_meta:
        if (
            col.get("csvw-safe:publicPartitions")
            and not col.get("csvw-safe:privacyId", False)
            and col.get("csvw-safe:maxInfluencedPartitions", 1) > 1
        ):
            col["csvw-safe:partitionKey"] = True


def make_partition_key(col_name, values, continuous=False, bin_edges=None):
    partitions = []

    if continuous:
        # bin_edges defines the ranges [(low, high, lower_inc, upper_inc), ...]
        for low, high, lower_inc, upper_inc in bin_edges:
            partitions.append({
                "@type": "csvw-safe:PartitionKey",
                "csvw-safe:lowerBound": float(low),
                "csvw-safe:upperBound": float(high),
                "csvw-safe:lowerInclusive": bool(lower_inc),
                "csvw-safe:upperInclusive": bool(upper_inc)
            })
    else:
        for val in values:
            partitions.append({
                "@type": "csvw-safe:PartitionKey",
                "csvw-safe:partitionValue": val
            })

    return partitions

# ----------------------------
# Optional ColumnGroup Detection
# ----------------------------

def detect_column_groups(df, columns_meta, individual_col):
    groups = []

    # Only columns with categorical publicPartitions
    candidate_cols = []
    for c in columns_meta:
        if c.get("csvw-safe:privacyId", False):
            continue

        partitions = c.get("csvw-safe:publicPartitions")
        if not partitions:
            continue

        # Check if first partition uses partitionValue (categorical)
        if isinstance(partitions, list) and \
           isinstance(partitions[0], dict) and \
           "csvw-safe:partitionValue" in partitions[0]:
            candidate_cols.append(c["name"])

    for c1, c2 in combinations(candidate_cols, 2):

        joint = df[[c1, c2]].dropna()
        if joint.empty:
            continue

        joint_unique = joint.drop_duplicates()
        if joint_unique.shape[0] <= 1:
            continue

        public_partitions = []

        for _, row in joint_unique.iterrows():
            public_partitions.append({
                "@type": "csvw-safe:PartitionKey",
                "csvw-safe:components": {
                    c1: {
                        "@type": "csvw-safe:PartitionKey",
                        "csvw-safe:partitionValue": row[c1]
                    },
                    c2: {
                        "@type": "csvw-safe:PartitionKey",
                        "csvw-safe:partitionValue": row[c2]
                    }
                }
            })

        groups.append({
            "@type": "csvw-safe:ColumnGroup",
            "csvw-safe:columns": [c1, c2],
            "csvw-safe:publicPartitions": public_partitions
        })

    return groups


# ----------------------------
# Tighten DP Bounds
# ----------------------------

def tighten_table_bounds(meta, df, individual_col):

    columns = meta["tableSchema"]["columns"]

    max_influenced = max(
        col.get("csvw-safe:maxInfluencedPartitions", 1)
        for col in columns
    )

    max_partition_contrib = max(
        col.get("csvw-safe:maxPartitionContribution", 1)
        for col in columns
    )

    derived_max_contrib = max(
        df.groupby(individual_col).size().max(),
        max_partition_contrib,
        max_influenced
    )

    meta["tableSchema"]["csvw-safe:maxInfluencedPartitions"] = int(max_influenced)
    meta["tableSchema"]["csvw-safe:maxPartitionContribution"] = int(max_partition_contrib)


# ----------------------------
# Main Generator
# ----------------------------

def make_metadata_from_data(
    df: pd.DataFrame,
    individual_col: str, # TODO: add option if None --> field related are empty (private_id field always false)
    max_contributions: int = 2,
    auto_partition_keys: bool = False,
    auto_column_groups: bool = False,
):

    if individual_col not in df.columns:
        raise ValueError(f"individual_col '{individual_col}' not found")

    meta = {
        "@context": [
            "http://www.w3.org/ns/csvw",
            "../../../csvw-safe-context.jsonld" # local path for dev (TODO later)
            #"https://w3id.org/csvw-dp#"
        ],
        "tableSchema": {
            "csvw-safe:maxContributions": int(max_contributions),
            "csvw-safe:maxTableLength": int(len(df)),
            "csvw-safe:tableLength": int(len(df)),
            "columns": []
        }
    }

    for col in df.columns:
        col_data = df[col]
        non_null = col_data.dropna()

        col_info = {
            "name": col,
            "datatype": csvw_dtype(col_data),
            "csvw-safe:privacyId": col == individual_col,
            "required": bool(col_data.isna().mean() == 0),
            "csvw-safe:nullableProportion": round(float(col_data.isna().mean()), 3),
        }

        if pd.api.types.is_bool_dtype(col_data):
            unique_vals = sorted(non_null.astype(bool).unique().tolist())
            col_info["csvw-safe:publicPartitions"] = make_partition_key(
                col,
                unique_vals,
                continuous=False
            )
            col_info.update(compute_margins(df, individual_col, col))

        elif pd.api.types.is_datetime64_any_dtype(col_data):
            if len(non_null):
                col_info["minimum"] = str(non_null.min())
                col_info["maximum"] = str(non_null.max())

        elif pd.api.types.is_numeric_dtype(col_data):
            if len(non_null):
                col_info["minimum"] = float(non_null.min())
                col_info["maximum"] = float(non_null.max())
            
            if is_categorical_int(col_data):
                col_info["datatype"] = "integer"
                unique_vals = sorted(non_null.astype(int).unique().tolist())
                col_info["csvw-safe:publicPartitions"] = make_partition_key(col, unique_vals)
                col_info.update(compute_margins(df, individual_col, col))
            else:
                col_info["datatype"] = "double"

                if len(non_null): # TODO: only advanced for specif column, add fields
                    min_val = float(np.floor(non_null.min()))
                    max_val = float(np.ceil(non_null.max()))
                    # Example: 2 bins
                    mid = (min_val + max_val) / 2
                    bins = [
                        (non_null.min(), mid, True, False),
                        (mid, non_null.max(), True, True)
                    ]
                    col_info["csvw-safe:publicPartitions"] = make_partition_key(col, None, continuous=True, bin_edges=bins)

        else:
            col_info["datatype"] = "string"
            unique_vals = sorted(non_null.astype(str).unique().tolist())
        
            col_info["csvw-safe:publicPartitions"] = make_partition_key(
                col,
                unique_vals,
                continuous=False
            )
        
            col_info.update(compute_margins(df, individual_col, col))

        meta["tableSchema"]["columns"].append(col_info)

    # ----------------------------
    # Automatic Enhancements
    # ----------------------------

    if auto_partition_keys:
        detect_partition_keys(meta["tableSchema"]["columns"])

    if auto_column_groups:
        groups = detect_column_groups(df, meta["tableSchema"]["columns"], individual_col)
        if groups:
            meta["tableSchema"]["csvw-safe:columnGroups"] = groups

    tighten_table_bounds(meta, df, individual_col)

    return meta

# ============================================================
# CLI
# ============================================================

def main():
    parser = argparse.ArgumentParser(
        description="Generate CSVW-DP metadata from CSV data"
    )

    parser.add_argument(
        "csv_file",
        type=str,
        help="Input CSV file"
    )

    parser.add_argument(
        "--id",
        required=True,
        type=str,
        help="Column name used as privacyId / individual identifier"
    )

    parser.add_argument(
        "--output",
        type=str,
        default="metadata.json",
        help="Output metadata JSON file (default: metadata.json)"
    )

    parser.add_argument(
        "--max-contributions",
        type=int,
        default=2,
        help="csvw-safe:maxContributions per individual in table"
    )

    parser.add_argument(
        "--no-auto-partition-keys",
        action="store_true",
        help="Disable automatic partition key detection"
    )

    parser.add_argument(
        "--auto-column-groups",
        action="store_true",
        help="Enable automatic column group detection"
    )

    args = parser.parse_args()

    csv_path = Path(args.csv_file)

    if not csv_path.exists():
        print(f"ERROR: File not found: {csv_path}")
        return

    try:
        df = pd.read_csv(csv_path)
    except Exception as e:
        print(f"ERROR: Could not read CSV: {e}")
        return

    try:
        metadata = make_metadata_from_data(
            df=df,
            individual_col=args.id,
            max_contributions=args.max_contributions,
            auto_partition_keys=not args.no_auto_partition_keys,
            auto_column_groups=args.auto_column_groups,
        )
    except Exception as e:
        print(f"ERROR: Failed to generate metadata: {e}")
        return

    output_path = Path(args.output)

    try:
        with output_path.open("w", encoding="utf-8") as f:
            json.dump(metadata, f, indent=2)
        print(f"Metadata written to {output_path}")
    except Exception as e:
        print(f"ERROR: Could not write output file: {e}")


if __name__ == "__main__":
    main()