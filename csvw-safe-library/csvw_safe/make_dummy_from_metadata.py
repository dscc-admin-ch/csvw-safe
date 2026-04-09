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
from typing import Any

import numpy as np
import pandas as pd

from csvw_safe.constants import (
    ADD_INFO,
    COL_LIST,
    COL_NAME,
    DATATYPE,
    DEPENDS_ON,
    EXHAUSTIVE_PARTITIONS,
    KEY_VALUES,
    LOWER_BOUND,
    NULL_PROP,
    PARTITION_VALUE,
    PREDICATE,
    PUBLIC_PARTITIONS,
    TABLE_SCHEMA,
    UPPER_BOUND,
)
from csvw_safe.datatypes import XSD_GROUP_MAP, DataTypes, DataTypesGroups
from csvw_safe.generate_series import generate_series

RANDOM_STRINGS = list(string.ascii_lowercase + string.ascii_uppercase + string.digits)


def apply_nulls_serie(
    series: pd.Series, nullable_prop: float, datatype: DataTypes, rng: np.random.Generator
) -> pd.Series:
    """
    Inject null values into a column according to metadata.

    Parameters
    ----------
    series : pd.Series
        Column values.
    nullable_prop : float
        Proportion of null values.
    datatype : DataTypes
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

    n_null = max(1, int(len(series) * nullable_prop))
    idx = rng.choice(series.index, size=n_null, replace=False)

    group = XSD_GROUP_MAP[datatype]
    if group == DataTypesGroups.DATETIME:
        series.loc[idx] = pd.NaT
    else:
        series.loc[idx] = pd.NA

    return series


def _apply_value_mask(series: pd.Series, value: Any) -> pd.Series:
    """Return mask for a categorical or continuous predicate."""
    if isinstance(value, dict):
        if PARTITION_VALUE in value:
            return series == value[PARTITION_VALUE]

        lower = value.get(LOWER_BOUND)
        upper = value.get(UPPER_BOUND)

        mask = pd.Series(True, index=series.index)
        mask &= series >= lower
        mask &= series <= upper
        return mask

    return series == value


def _predicate_mask(df: pd.DataFrame, predicate: dict[str, Any]) -> pd.Series:
    """Return mask for a full predicate."""
    mask = pd.Series(True, index=df.index)
    for col, value in predicate.items():
        mask &= _apply_value_mask(df[col], value)
    return mask


def column_group_partitions(
    df: pd.DataFrame,
    columns_group_meta: list[dict[str, Any]],
) -> pd.DataFrame:
    """Keep only rows belonging to allowed column-group partitions."""
    global_mask = pd.Series(True, index=df.index)
    for col_group in columns_group_meta:
        if not col_group.get(EXHAUSTIVE_PARTITIONS, False):
            continue

        partitions = col_group.get(PUBLIC_PARTITIONS) or col_group.get(KEY_VALUES, [])
        group_mask = pd.Series(False, index=df.index)
        for p in partitions:
            predicate = p.get(PREDICATE, p)
            group_mask |= _predicate_mask(df, predicate)

        global_mask &= group_mask

    return df[global_mask].reset_index(drop=True)


def apply_nulls_dataframe(
    df: pd.DataFrame, columns_meta: list[dict[str, Any]], rng: np.random.Generator
) -> pd.DataFrame:
    """Apply null proportion on dataframe."""
    columns_meta_map = {c[COL_NAME]: c for c in columns_meta}
    for col in df.columns:
        series = df[col]
        col_meta = columns_meta_map[col]
        nullable_prop = col_meta.get(NULL_PROP, 0)
        datatype = col_meta[DATATYPE]
        df[col] = apply_nulls_serie(series, nullable_prop, datatype, rng)
    return df


def make_dummy_from_metadata(
    metadata: dict[str, Any],
    nb_rows: int = 100,
    seed: int = 0,
) -> pd.DataFrame:
    """
    Generate a dummy dataset from CSVW-SAFE metadata, respecting exhaustive column group partitions.

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
        Generated dataset
    """
    rng = np.random.default_rng(seed)

    columns_meta = metadata[TABLE_SCHEMA][COL_LIST]

    depends_map = {col_meta[COL_NAME]: col_meta.get(DEPENDS_ON) for col_meta in columns_meta}
    columns_group_meta = metadata.get(ADD_INFO, [])

    generated: list[pd.DataFrame] = []
    while sum(len(df) for df in generated) < nb_rows:
        data_dict: dict[str, pd.Series] = {}
        for col_meta in columns_meta:
            data_dict = generate_series(
                col_meta[COL_NAME],
                columns_meta,
                depends_map,
                data_dict,
                nb_rows,
                rng,
            )

        df = pd.DataFrame(data_dict)

        if columns_group_meta:
            df = column_group_partitions(df, columns_group_meta)

        generated.append(df)

    output_df = pd.concat(generated, ignore_index=True)
    output_df = output_df.sample(n=nb_rows, random_state=seed)  # final row selection

    output_df = apply_nulls_dataframe(output_df, columns_meta, rng)

    return output_df.reset_index(drop=True)


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
        f"Dummy dataset written to {args.output} ({len(df_dummy)} rows,"
        f"{len(df_dummy.columns)} columns)."
    )


if __name__ == "__main__":
    main()
