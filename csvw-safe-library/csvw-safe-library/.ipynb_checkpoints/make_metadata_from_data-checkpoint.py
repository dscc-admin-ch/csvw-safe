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
        "dp:maxNumPartitions": int(df[col].nunique(dropna=True)),
        "dp:maxPartitionLength": int(col_data.value_counts().max() if len(col_data) else 0),
        "dp:maxInfluencedPartitions": int(
            df.groupby(individual_col)[col].nunique(dropna=True).max()
        ),
        "dp:maxPartitionContribution": int(
            df.groupby([individual_col, col], observed=True).size().max()
        ),
    }


# ----------------------------
# Partition Key Detection
# ----------------------------

def detect_partition_keys(columns_meta):
    for col in columns_meta:
        if (
            col.get("dp:publicPartitions")
            and not col.get("dp:privacyId", False)
            and col.get("dp:maxInfluencedPartitions", 1) > 1
        ):
            col["dp:partitionKey"] = True


# ----------------------------
# Optional ColumnGroup Detection
# ----------------------------

def detect_column_groups(df, columns_meta, individual_col):
    groups = []
    candidate_cols = [
        c["name"]
        for c in columns_meta
        if c.get("dp:publicPartitions") and not c.get("dp:privacyId", False)
    ]

    for c1, c2 in combinations(candidate_cols, 2):
        joint = df[[c1, c2]].dropna()

        if len(joint) == 0:
            continue

        # Only create group if joint partitions are meaningful
        if joint.drop_duplicates().shape[0] > 1:
            groups.append({
                "dp:columns": [c1, c2],
                "dp:maxNumPartitions": int(joint.drop_duplicates().shape[0])
            })

    return groups


# ----------------------------
# Tighten DP Bounds
# ----------------------------

def tighten_table_bounds(meta, df, individual_col, strict):

    columns = meta["tableSchema"]["columns"]

    max_influenced = max(
        col.get("dp:maxInfluencedPartitions", 1)
        for col in columns
    )

    max_partition_contrib = max(
        col.get("dp:maxPartitionContribution", 1)
        for col in columns
    )

    derived_max_contrib = max(
        df.groupby(individual_col).size().max(),
        max_partition_contrib,
        max_influenced
    )

    if strict:
        meta["tableSchema"]["dp:maxContributions"] = int(derived_max_contrib)

    meta["tableSchema"]["dp:maxInfluencedPartitions"] = int(max_influenced)
    meta["tableSchema"]["dp:maxPartitionContribution"] = int(max_partition_contrib)


# ----------------------------
# Main Generator
# ----------------------------

def generate_csvw_dp_metadata(
    df: pd.DataFrame,
    csv_url: str,
    individual_col: str,
    max_contributions: int = 2,
    mode: str = "relaxed",
    auto_partition_keys: bool = True,
    auto_column_groups: bool = False,
):

    if individual_col not in df.columns:
        raise ValueError(f"individual_col '{individual_col}' not found")

    strict = mode == "strict"

    meta = {
        "@context": [
            "http://www.w3.org/ns/csvw",
            "https://w3id.org/csvw-dp#"
        ],
        "url": csv_url,
        "tableSchema": {
            "dp:maxContributions": int(max_contributions),
            "dp:maxTableLength": int(len(df)),
            "dp:tableLength": int(len(df)),
            "columns": []
        }
    }

    for col in df.columns:
        col_data = df[col]
        non_null = col_data.dropna()

        col_info = {
            "name": col,
            "datatype": csvw_dtype(col_data),
            "dp:privacyId": col == individual_col,
            "required": col_data.isna().mean() == 0,
            "dp:nullableProportion": round(float(col_data.isna().mean()), 3),
        }

        if pd.api.types.is_bool_dtype(col_data):
            col_info["dp:publicPartitions"] = [True, False]
            col_info.update(compute_margins(df, individual_col, col))

        elif pd.api.types.is_datetime64_any_dtype(col_data):
            if len(non_null):
                col_info["minimum"] = str(non_null.min())
                col_info["maximum"] = str(non_null.max())

        elif pd.api.types.is_numeric_dtype(col_data):
            if is_categorical_int(col_data):
                col_info["datatype"] = "integer"
                col_info["dp:publicPartitions"] = sorted(
                    non_null.astype(int).unique().tolist()
                )
                col_info.update(compute_margins(df, individual_col, col))
            else:
                col_info["datatype"] = "double"
                if len(non_null):
                    col_info["minimum"] = float(np.floor(non_null.min()))
                    col_info["maximum"] = float(np.ceil(non_null.max()))

        else:
            col_info["datatype"] = "string"
            col_info["dp:publicPartitions"] = sorted(
                non_null.astype(str).unique().tolist()
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
            meta["tableSchema"]["dp:columnGroups"] = groups

    tighten_table_bounds(meta, df, individual_col, strict)

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
        "--mode",
        choices=["relaxed", "strict"],
        default="relaxed",
        help="Metadata generation mode (default: relaxed)"
    )

    parser.add_argument(
        "--max-contributions",
        type=int,
        default=2,
        help="Initial dp:maxContributions (strict mode may override)"
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
        metadata = generate_csvw_dp_metadata(
            df=df,
            csv_url=str(csv_path.name),
            individual_col=args.id,
            max_contributions=args.max_contributions,
            mode=args.mode,
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