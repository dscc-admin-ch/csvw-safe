#!/usr/bin/env python3
import numpy as np
import pandas as pd
import json
from pathlib import Path
import argparse


# ============================================================
# Helpers
# ============================================================

def sample_from_partitions(partitions, nb_rows, rng):
    """
    Sample values from csvw-safe:publicPartitions
    Handles both categorical and continuous partitions.
    """
    if not partitions:
        return pd.Series([pd.NA] * nb_rows)

    first = partitions[0]

    # ----------------------------------
    # CATEGORICAL partitions
    # ----------------------------------
    if "csvw-safe:partitionValue" in first:
        values = [
            p["csvw-safe:partitionValue"]
            for p in partitions
        ]
        return pd.Series(rng.choice(values, size=nb_rows))

    # ----------------------------------
    # CONTINUOUS partitions
    # ----------------------------------
    if "csvw-safe:lowerBound" in first:
        samples = []
        for _ in range(nb_rows):
            p = rng.choice(partitions)
            low = p["csvw-safe:lowerBound"]
            high = p["csvw-safe:upperBound"]
            val = rng.uniform(low, high)
            samples.append(val)
        return pd.Series(samples)

    return pd.Series([pd.NA] * nb_rows)


def apply_nulls(series, nullable_prop, dtype, rng):
    if nullable_prop <= 0:
        return series

    n_null = int(len(series) * nullable_prop)
    if n_null == 0:
        return series

    idx = rng.choice(series.index, size=n_null, replace=False)

    if dtype == "dateTime":
        series.loc[idx] = pd.NaT
    else:
        series.loc[idx] = pd.NA

    return series


# ============================================================
# Main Generator
# ============================================================

def make_dummy_from_metadata(metadata: dict, nb_rows: int = 100, seed: int = 0):
    rng = np.random.default_rng(seed)

    table = metadata["tableSchema"]
    columns_meta = table["columns"]

    data_dict = {}
    used_columns = set()

    # --------------------------------------------------------
    # 1️⃣ Handle columnGroups FIRST
    # --------------------------------------------------------
    for group in table.get("csvw-safe:columnGroups", []):
        cols = group["csvw-safe:columns"]
        partitions = group.get("csvw-safe:publicPartitions", [])

        if not partitions:
            continue

        sampled_groups = rng.choice(partitions, size=nb_rows)

        for col in cols:
            values = []
            for p in sampled_groups:
                component = p["csvw-safe:components"][col]
                values.append(component["csvw-safe:partitionValue"])
            data_dict[col] = pd.Series(values)
            used_columns.add(col)

    # --------------------------------------------------------
    # 2️⃣ Handle remaining columns
    # --------------------------------------------------------
    for col_meta in columns_meta:

        name = col_meta["name"]

        if name in used_columns:
            continue

        dtype = col_meta["datatype"]
        nullable_prop = col_meta.get("csvw-safe:nullableProportion", 0)
        partitions = col_meta.get("csvw-safe:publicPartitions", [])

        # ----------------------------------
        # DateTime
        # ----------------------------------
        if dtype == "dateTime":
            if "minimum" in col_meta and "maximum" in col_meta:
                dates = pd.date_range(
                    start=col_meta["minimum"],
                    end=col_meta["maximum"],
                )
                series = pd.Series(rng.choice(dates, size=nb_rows))
            else:
                series = pd.Series([pd.NaT] * nb_rows)

        # ----------------------------------
        # Numeric without partitions
        # ----------------------------------
        elif dtype in ("double", "integer") and not partitions:
            if "minimum" in col_meta and "maximum" in col_meta:
                low = col_meta["minimum"]
                high = col_meta["maximum"]

                if dtype == "integer":
                    series = pd.Series(
                        rng.integers(int(low), int(high) + 1, size=nb_rows)
                    )
                else:
                    series = pd.Series(
                        rng.uniform(float(low), float(high), size=nb_rows)
                    )
            else:
                series = pd.Series([pd.NA] * nb_rows)

        # ----------------------------------
        # Columns with partitions
        # ----------------------------------
        else:
            series = sample_from_partitions(partitions, nb_rows, rng)

            if dtype == "integer":
                series = series.astype("Int64")

        # Apply null proportion
        series = apply_nulls(series, nullable_prop, dtype, rng)

        data_dict[name] = series
        used_columns.add(name)

    return pd.DataFrame(data_dict)


# ============================================================
# CLI
# ============================================================

def main():
    parser = argparse.ArgumentParser(
        description="Generate dummy dataset from CSVW-SAFE metadata"
    )

    parser.add_argument("metadata_file", type=str)
    parser.add_argument("--rows", type=int, default=100)
    parser.add_argument("--output", type=str, default="dummy.csv")
    parser.add_argument("--seed", type=int, default=0)

    args = parser.parse_args()

    metadata_path = Path(args.metadata_file)

    if not metadata_path.exists():
        print(f"ERROR: Metadata file not found: {metadata_path}")
        return

    with metadata_path.open("r", encoding="utf-8") as f:
        metadata = json.load(f)

    df_dummy = make_dummy_from_metadata(
        metadata,
        nb_rows=args.rows,
        seed=args.seed
    )

    df_dummy.to_csv(args.output, index=False)

    print(
        f"Dummy dataset written to {args.output} "
        f"({len(df_dummy)} rows, {len(df_dummy.columns)} columns)"
    )


if __name__ == "__main__":
    main()