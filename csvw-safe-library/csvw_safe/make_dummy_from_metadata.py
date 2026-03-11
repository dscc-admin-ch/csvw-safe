"""
CSVW-SAFE Dummy Dataset Generator.

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
import string
from pathlib import Path
from typing import Any, Dict

import numpy as np
import pandas as pd

from csvw_safe.constants import (  # LOWER_BOUND,; UPPER_BOUND,
    DEFAULT_NUMBER_PARTITIONS,
    EXHAUSTIVE_PARTITIONS,
    MAX_NUM_PARTITIONS,
    MAXIMUM,
    MINIMUM,
    NULL_PROP,
    PARTITION_VALUE,
    PREDICATE,
    PUBLIC_PARTITIONS,
)
from csvw_safe.datatypes import DataTypes, T

RANDOM_STRINGS = list(string.ascii_lowercase + string.ascii_uppercase + string.digits)


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

    if datatype == DataTypes.DATETIME:
        series.loc[idx] = pd.NaT
    else:
        series.loc[idx] = pd.NA

    return series


def get_bounds(col_meta: Dict[str, Any]) -> tuple[T, T]:
    """Get min and max."""
    assert MINIMUM in col_meta, "error"
    assert MAXIMUM in col_meta, "error"
    return col_meta[MINIMUM], col_meta[MAXIMUM]


def generate_datetime_column(
    col_meta: Dict[str, Any], nb_rows: int, rng: np.random.Generator
) -> pd.Series:
    """Generate datetime column between min and max values."""
    lower, upper = get_bounds(col_meta)
    dates = pd.date_range(start=lower, end=upper)
    return pd.Series(rng.choice(dates, size=nb_rows))


def generate_integer_column(
    col_meta: Dict[str, Any], nb_rows: int, rng: np.random.Generator
) -> pd.Series:
    """Generate numeric column integer between min and max values."""
    lower, upper = get_bounds(col_meta)
    return pd.Series(rng.integers(int(lower), int(upper) + 1, size=nb_rows))


def generate_double_column(
    col_meta: Dict[str, Any], nb_rows: int, rng: np.random.Generator
) -> pd.Series:
    """Generate numeric column double between min and max values."""
    lower, upper = get_bounds(col_meta)
    return pd.Series(rng.uniform(float(lower), float(upper), size=nb_rows))


def generate_boolean_column(
    col_meta: Dict[str, Any], nb_rows: int, rng: np.random.Generator
) -> pd.Series:
    """Generate boolean column."""
    return pd.Series(rng.choice([True, False], size=nb_rows), dtype="boolean")


def generate_string_column(
    col_meta: Dict[str, Any], nb_rows: int, rng: np.random.Generator
) -> pd.Series:
    """Generate string column depending on available information."""
    public_keys = []
    if PUBLIC_PARTITIONS in col_meta:
        for partition in col_meta[PUBLIC_PARTITIONS]:
            if isinstance(partition, str):
                public_keys.append(partition)
            else:
                public_keys.append(partition[PREDICATE][PARTITION_VALUE])
        if EXHAUSTIVE_PARTITIONS in col_meta and not col_meta[EXHAUSTIVE_PARTITIONS]:
            diff = col_meta[MAX_NUM_PARTITIONS] - len(col_meta[PUBLIC_PARTITIONS])
            public_keys.extend(RANDOM_STRINGS[0:diff])
    else:
        if MAX_NUM_PARTITIONS in col_meta and col_meta[MAX_NUM_PARTITIONS]:
            public_keys = RANDOM_STRINGS[0 : col_meta[MAX_NUM_PARTITIONS]]
        else:
            public_keys = RANDOM_STRINGS[0:DEFAULT_NUMBER_PARTITIONS]

    return pd.Series(rng.choice(public_keys, size=nb_rows))


def generate_column_series(
    col_meta: Dict[str, Any],
    nb_rows: int,
    rng: np.random.Generator,
) -> pd.Series:
    """
    Generate a single column series based on metadata.

    Handles datetime, numeric, and partitioned columns, applying nulls.
    """
    datatype = col_meta["datatype"]
    nullable_prop = col_meta.get(NULL_PROP, 0)

    if datatype == DataTypes.DATETIME:
        series = generate_datetime_column(col_meta, nb_rows, rng)
    elif datatype in DataTypes.INTEGER:
        series = generate_integer_column(col_meta, nb_rows, rng)
    elif datatype in DataTypes.DOUBLE:
        series = generate_double_column(col_meta, nb_rows, rng)
    elif datatype == DataTypes.BOOLEAN:
        series = generate_boolean_column(col_meta, nb_rows, rng)
    elif datatype == DataTypes.STRING:
        series = generate_string_column(col_meta, nb_rows, rng)
    else:
        raise ValueError(f"Unknow datatype {datatype}")

    series = apply_nulls(series, nullable_prop, datatype, rng)
    return series


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

    # Single columns
    for col_meta in columns_meta:
        if col_meta.get("@type") != "csvw:Column":
            continue
        name = col_meta["name"]
        print(name)
        data_dict[name] = generate_column_series(col_meta, nb_rows, rng)

    # Column groups (joint partitions): TODO

    return pd.DataFrame(data_dict)


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
    parser = argparse.ArgumentParser(
        description="Generate a dummy dataset from CSVW-SAFE metadata."
    )

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
        f"Dummy dataset written to {args.output} "
        f"({len(df_dummy)} rows, {len(df_dummy.columns)} columns)"
    )


if __name__ == "__main__":
    main()
