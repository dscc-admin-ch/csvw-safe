#!/usr/bin/env python3
import numpy as np
import pandas as pd
import json
from pathlib import Path
import argparse
from itertools import product


def sample_partition_key(col_meta, nb_rows, rng):
    """Sample a column respecting dp:publicPartitions and nulls."""
    nullable_prop = col_meta.get("dp:nullableProportion", 0)
    dtype = col_meta["datatype"]
    name = col_meta["name"]

    serie = None
    if dtype in ("string", "integer", "boolean"):
        choices = col_meta.get("dp:publicPartitions", [])
        if not choices:
            raise ValueError(f"No publicPartitions for partitionKey '{name}'")
        serie = pd.Series(rng.choice(choices, size=nb_rows), dtype="object" if dtype=="string" else None)
    elif dtype == "double":
        low = float(col_meta["minimum"])
        high = float(col_meta["maximum"])
        serie = pd.Series(low + (high - low) * rng.random(size=nb_rows))
    elif dtype == "dateTime":
        dates = pd.date_range(start=col_meta["minimum"], end=col_meta["maximum"])
        serie = pd.Series(rng.choice(dates, size=nb_rows))
    else:
        raise ValueError(f"Unsupported dtype '{dtype}' for partitionKey '{name}'")

    # Apply nulls
    if nullable_prop > 0:
        n_null = int(nb_rows * nullable_prop)
        if n_null > 0:
            idx = rng.choice(serie.index, size=n_null, replace=False)
            if dtype == "dateTime":
                serie.loc[idx] = pd.NaT
            else:
                serie.loc[idx] = pd.NA
    return serie


def make_dummy_from_metadata(metadata: dict, nb_rows: int = 100, seed: int = 0) -> pd.DataFrame:
    """
    Create a dummy dataset from CSVW-DP metadata, respecting partitionKeys and columnGroups.
    """
    rng = np.random.default_rng(seed)
    data_dict = {}
    columns_meta = {c["name"]: c for c in metadata["tableSchema"]["columns"]}
    used_columns = set()

    # ----------------------------
    # Handle columnGroups first
    # ----------------------------
    for group in metadata["tableSchema"].get("dp:columnGroups", []):
        cols = group["dp:columns"]
        # Use publicPartitions if available
        partition_values = []
        for c in cols:
            c_meta = columns_meta[c]
            pvals = c_meta.get("dp:publicPartitions")
            if not pvals:
                # fallback to uniform numeric sampling
                if c_meta["datatype"] == "integer":
                    pvals = list(range(int(c_meta["minimum"]), int(c_meta["maximum"])+1))
                elif c_meta["datatype"] == "double":
                    pvals = np.linspace(c_meta["minimum"], c_meta["maximum"], min(nb_rows, 20)).tolist()
                elif c_meta["datatype"] == "string":
                    pvals = ["A","B","C"]
                elif c_meta["datatype"] == "boolean":
                    pvals = [True, False]
            partition_values.append(pvals)

        # Sample tuples from Cartesian product
        choices = list(product(*partition_values))
        sampled = rng.choice(len(choices), size=nb_rows)
        for i, col in enumerate(cols):
            data_dict[col] = pd.Series([choices[s][i] for s in sampled])
            used_columns.add(col)

    # ----------------------------
    # Handle partitionKeys not in groups
    # ----------------------------
    for col_name, col_meta in columns_meta.items():
        if col_name in used_columns:
            continue
        if col_meta.get("dp:partitionKey", False):
            data_dict[col_name] = sample_partition_key(col_meta, nb_rows, rng)
            used_columns.add(col_name)

    # ----------------------------
    # Handle remaining columns
    # ----------------------------
    for col_name, col_meta in columns_meta.items():
        if col_name in used_columns:
            continue
        data_dict[col_name] = sample_partition_key(col_meta, nb_rows, rng)
        used_columns.add(col_name)

    return pd.DataFrame(data_dict)


# ----------------------------
# CLI
# ----------------------------
def main():
    parser = argparse.ArgumentParser(description="Generate dummy dataset from CSVW-DP metadata")
    parser.add_argument("metadata_file", type=str, help="Input JSON metadata file")
    parser.add_argument("--rows", type=int, default=100, help="Number of rows to generate")
    parser.add_argument("--output", type=str, default="dummy.csv", help="Output CSV file")
    parser.add_argument("--seed", type=int, default=0, help="Random seed")
    args = parser.parse_args()

    metadata_path = Path(args.metadata_file)
    if not metadata_path.exists():
        print(f"ERROR: Metadata file not found: {metadata_path}")
        return

    with metadata_path.open("r", encoding="utf-8") as f:
        metadata = json.load(f)

    df_dummy = make_dummy_from_metadata(metadata, nb_rows=args.rows, seed=args.seed)

    df_dummy.to_csv(args.output, index=False)
    print(f"Dummy dataset written to {args.output} ({len(df_dummy)} rows, {len(df_dummy.columns)} columns)")


if __name__ == "__main__":
    main()