"""
CSVW-SAFE Dummy Dataset Generator

This module generates a synthetic dummy dataset from CSVW-SAFE metadata.
It is intended for testing pipelines and validating metadata structures.

The generator supports:
- categorical partitions
- numeric partitions
- datetime ranges
- nullable proportions
- column groups (joint partitions)

The resulting dataset respects the structural information contained in
CSVW-SAFE metadata but does not guarantee semantic correctness.
"""

import argparse
import json
from pathlib import Path
from typing import Any, Dict, List

import numpy as np
import pandas as pd


# ============================================================
# Utilities
# ============================================================
def sample_from_partitions(
    partitions: List[Dict[str, Any]],
    nb_rows: int,
    rng: np.random.Generator,
) -> pd.Series:
    """
    Sample values from CSVW-SAFE partitions.

    Supports both categorical and continuous partitions.

    Parameters
    ----------
    partitions : list of dict
        Partition metadata objects.
    nb_rows : int
        Number of samples to generate.
    rng : numpy.random.Generator
        Random number generator.

    Returns
    -------
    pandas.Series
        Sampled values.
    """
    if not partitions:
        return pd.Series([pd.NA] * nb_rows)

    predicate = partitions[0].get("csvw-safe:predicate", {})

    # --------------------------------------------------------
    # Categorical partitions
    # --------------------------------------------------------
    if "partitionValue" in predicate:
        values = [p["csvw-safe:predicate"]["partitionValue"] for p in partitions]
        return pd.Series(rng.choice(values, size=nb_rows))

    # --------------------------------------------------------
    # Continuous partitions
    # --------------------------------------------------------
    if "lowerBound" in predicate:
        samples = []

        for _ in range(nb_rows):
            p = partitions[rng.integers(len(partitions))]
            pred = p["csvw-safe:predicate"]

            low = pred["lowerBound"]
            high = pred["upperBound"]

            samples.append(rng.uniform(low, high))

        return pd.Series(samples)

    return pd.Series([pd.NA] * nb_rows)


def apply_nulls(
    series: pd.Series,
    nullable_prop: float,
    datatype: str,
    rng: np.random.Generator,
) -> pd.Series:
    """
    Inject null values into a column according to metadata.

    Parameters
    ----------
    series : pd.Series
        Column values.
    nullable_prop : float
        Proportion of null values.
    datatype : str
        Column datatype.
    rng : numpy.random.Generator
        Random number generator.

    Returns
    -------
    pandas.Series
        Column with nulls applied.
    """
    if nullable_prop <= 0:
        return series

    n_null = int(len(series) * nullable_prop)
    if n_null == 0:
        return series

    idx = rng.choice(series.index, size=n_null, replace=False)

    if datatype == "dateTime":
        series.loc[idx] = pd.NaT
    else:
        series.loc[idx] = pd.NA

    return series


# ============================================================
# Generator
# ============================================================
def make_dummy_from_metadata(
    metadata: Dict[str, Any],
    nb_rows: int = 100,
    seed: int = 0,
) -> pd.DataFrame:
    """
    Generate a dummy dataset from CSVW-SAFE metadata.

    Parameters
    ----------
    metadata : dict
        CSVW-SAFE metadata structure.
    nb_rows : int, default=100
        Number of rows to generate.
    seed : int, default=0
        Random seed.

    Returns
    -------
    pandas.DataFrame
        Generated dataset.
    """
    rng = np.random.default_rng(seed)

    table_schema = metadata.get("csvw:tableSchema", {})
    columns_meta = table_schema.get("columns", [])

    data_dict: Dict[str, pd.Series] = {}
    used_columns = set()

    # --------------------------------------------------------
    # Column groups (joint partitions)
    # --------------------------------------------------------
    for group in metadata.get("csvw-safe:additionalInformation", []):
        if group.get("@type") != "csvw-safe:ColumnGroup":
            continue

        cols = group.get("csvw-safe:columns", [])
        partitions = group.get("csvw-safe:public.partitions", [])

        if not partitions:
            continue

        idx = rng.integers(0, len(partitions), size=nb_rows)
        sampled_partitions = [partitions[i] for i in idx]

        group_data: Dict[str, List[Any]] = {col: [] for col in cols}

        for p in sampled_partitions:
            predicate = p.get("csvw-safe:predicate", {})

            for col in cols:
                col_pred = predicate.get(col, {})

                if "partitionValue" in col_pred:
                    group_data[col].append(col_pred["partitionValue"])

                elif "lowerBound" in col_pred:
                    low = col_pred["lowerBound"]
                    high = col_pred["upperBound"]
                    group_data[col].append(rng.uniform(low, high))

                else:
                    group_data[col].append(pd.NA)

        for col in cols:
            data_dict[col] = pd.Series(group_data[col])
            used_columns.add(col)

    # --------------------------------------------------------
    # Remaining columns
    # --------------------------------------------------------
    for col_meta in columns_meta:

        name = col_meta["name"]

        if name in used_columns:
            continue

        datatype = col_meta["datatype"]
        nullable_prop = col_meta.get("csvw-safe:synth.nullableProportion", 0)
        partitions = col_meta.get("csvw-safe:public.partitions", [])

        # ----------------------------------------------------
        # DateTime
        # ----------------------------------------------------
        if datatype == "dateTime":

            if "minimum" in col_meta and "maximum" in col_meta:

                dates = pd.date_range(
                    start=col_meta["minimum"],
                    end=col_meta["maximum"],
                )

                series = pd.Series(rng.choice(dates, size=nb_rows))

            else:
                series = pd.Series([pd.NaT] * nb_rows)

        # ----------------------------------------------------
        # Numeric without partitions
        # ----------------------------------------------------
        elif datatype in ("double", "integer") and not partitions:

            if "minimum" in col_meta and "maximum" in col_meta:

                low = col_meta["minimum"]
                high = col_meta["maximum"]

                if datatype == "integer":
                    series = pd.Series(rng.integers(int(low), int(high) + 1, size=nb_rows))
                else:
                    series = pd.Series(rng.uniform(float(low), float(high), size=nb_rows))

            else:
                series = pd.Series([pd.NA] * nb_rows)

        # ----------------------------------------------------
        # Columns with partitions
        # ----------------------------------------------------
        else:

            series = sample_from_partitions(partitions, nb_rows, rng)

            if datatype == "integer":
                series = series.astype("Int64")

        # Apply nullable proportion
        series = apply_nulls(series, nullable_prop, datatype, rng)

        data_dict[name] = series
        used_columns.add(name)

    return pd.DataFrame(data_dict)


# ============================================================
# CLI
# ============================================================
def main() -> None:
    """
    Command-line interface for dummy dataset generation.

    Parameters
    ----------
    metadata_file : str
        Path to CSVW-SAFE metadata JSON file.

    Optional parameters
    -------------------
    --rows : int
        Number of rows to generate (default: 100)

    --output : str
        Output CSV file (default: dummy.csv)

    --seed : int
        Random seed (default: 0)
    """
    parser = argparse.ArgumentParser(description="Generate a dummy dataset from CSVW-SAFE metadata.")

    parser.add_argument("metadata_file", type=str)
    parser.add_argument("--rows", type=int, default=100)
    parser.add_argument("--output", type=str, default="dummy.csv")
    parser.add_argument("--seed", type=int, default=0)

    args = parser.parse_args()

    metadata_path = Path(args.metadata_file)

    if not metadata_path.exists():
        raise FileNotFoundError(f"Metadata file not found: {metadata_path}")

    with metadata_path.open("r", encoding="utf-8") as f:
        metadata = json.load(f)

    df_dummy = make_dummy_from_metadata(
        metadata,
        nb_rows=args.rows,
        seed=args.seed,
    )

    df_dummy.to_csv(args.output, index=False)

    print(
        f"Dummy dataset written to {args.output} " f"({len(df_dummy)} rows, {len(df_dummy.columns)} columns)"
    )


if __name__ == "__main__":
    main()
