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
from typing import Any, Dict, List

import numpy as np
import pandas as pd

from csvw_safe.constants import (
    ADD_INFO,
    COL_LIST,
    COL_NAME,
    DATATYPE,
    DEFAULT_NUMBER_PARTITIONS,
    DEPENDENCY_TYPE,
    DEPENDS_ON,
    EXHAUSTIVE_PARTITIONS,
    LOWER_BOUND,
    MAX_NUM_PARTITIONS,
    MAXIMUM,
    MINIMUM,
    NULL_PROP,
    OVERSAMPLING_FACTOR,
    PARTITION_VALUE,
    PREDICATE,
    PUBLIC_PARTITIONS,
    TABLE_SCHEMA,
    UPPER_BOUND,
    VALUE_MAP,
    DependencyType,
)
from csvw_safe.datatypes import DataTypes, T, to_pandas_dtype

RANDOM_STRINGS = list(string.ascii_lowercase + string.ascii_uppercase + string.digits)


def apply_nulls(
    series: pd.Series, nullable_prop: float, datatype: str, rng: np.random.Generator
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

    n_null = max(1, int(len(series) * nullable_prop))  # at least one
    idx = rng.choice(series.index, size=n_null, replace=False)

    if datatype == DataTypes.DATETIME:
        series.loc[idx] = pd.NaT
    else:
        series.loc[idx] = pd.NA

    return series


def get_bounds(col_meta: Dict[str, Any]) -> tuple[T, T]:
    """Get min and max."""
    assert MINIMUM in col_meta, f"Missing {MINIMUM} in column {col_meta[COL_NAME]}"
    assert MAXIMUM in col_meta, f"Missing {MAXIMUM} in column {col_meta[COL_NAME]}"
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


def generate_boolean_column(nb_rows: int, rng: np.random.Generator) -> pd.Series:
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
    datatype = col_meta[DATATYPE]
    nullable_prop = col_meta.get(NULL_PROP, 0)

    if datatype == DataTypes.DATETIME:
        series = generate_datetime_column(col_meta, nb_rows, rng)
    elif datatype in DataTypes.INTEGER:
        series = generate_integer_column(col_meta, nb_rows, rng)
    elif datatype in DataTypes.DOUBLE:
        series = generate_double_column(col_meta, nb_rows, rng)
    elif datatype == DataTypes.BOOLEAN:
        series = generate_boolean_column(nb_rows, rng)
    elif datatype == DataTypes.STRING:
        series = generate_string_column(col_meta, nb_rows, rng)
    else:
        raise ValueError(f"Unknow datatype {datatype}")

    series = apply_nulls(series, nullable_prop, datatype, rng)
    return series


def _bigger_series(
    depend_serie: pd.Series,
    col_meta: Dict[str, Any],
    nb_rows: int,
    rng: np.random.Generator,
) -> pd.Series:
    """
    Generate a series where each value is greater than the corresponding value.

    in `depend_serie`, while respecting the original column bounds.

    Works for numeric and datetime columns.

    Parameters
    ----------
    depend_serie : pd.Series
        The series this column depends on.
    col_meta : dict
        Metadata for the column, must contain bounds and datatype.
    nb_rows : int
        Number of rows to generate.
    rng : np.random.Generator
        Random number generator for offsets.

    Returns
    -------
    pd.Series
        Generated series satisfying BIGGER dependency.
    """
    lower, upper = get_bounds(col_meta)
    datatype = col_meta[DATATYPE]

    if pd.api.types.is_datetime64_any_dtype(depend_serie):
        lower_dt, upper_dt = pd.to_datetime(lower), pd.to_datetime(upper)
        total_seconds = (upper_dt - lower_dt).total_seconds()
        # random offset between 1% and 20% of total range
        seconds_offsets = rng.integers(
            int(0.01 * total_seconds), int(0.2 * total_seconds), size=nb_rows
        )
        series = depend_serie + pd.to_timedelta(seconds_offsets, unit="s")
        return series.clip(lower=lower_dt, upper=upper_dt)

    if pd.api.types.is_numeric_dtype(depend_serie):
        offsets = rng.uniform(0.01, 0.2, size=nb_rows) * (upper - lower)
        series = depend_serie + offsets
        return series.clip(lower=lower, upper=upper)

    raise ValueError(f"BIGGER dependency not supported for datatype {datatype}")


def _mapping_series(
    depend_serie: pd.Series,
    col_meta: Dict[str, Any],
    rng: np.random.Generator,
) -> pd.Series:
    """
    Generate a series based on a valueMap dependency.

    Each value in depend_serie is mapped according to col_meta[VALUE_MAP].
    If multiple options exist, one is chosen randomly.

    Parameters
    ----------
    depend_serie : pd.Series
        Series to map from.
    col_meta : dict
        Column metadata, must include VALUE_MAP and DATATYPE.
    rng : np.random.Generator
        Random number generator for choosing among multiple mapping values.

    Returns
    -------
    pd.Series
        Generated series satisfying MAPPING dependency.
    """
    mapped = [
        (
            rng.choice(col_meta[VALUE_MAP][val])
            if isinstance(col_meta[VALUE_MAP].get(val), list)
            else col_meta[VALUE_MAP].get(val, pd.NA)
        )
        for val in depend_serie
    ]
    return pd.Series(mapped, dtype=to_pandas_dtype(col_meta[DATATYPE]))


def _fixed_series(
    depend_serie: pd.Series,
    col_meta: Dict[str, Any],
    rng: np.random.Generator,
) -> pd.Series:
    """
    Generate a series where each unique entity in depend_serie has a fixed value.

    (multi-row fixedPerEntity dependency).

    Parameters
    ----------
    depend_serie : pd.Series
        Entity identifier series.
    col_meta : dict
        Column metadata, must include DATATYPE.
    rng : np.random.Generator
        Random number generator for value generation.

    Returns
    -------
    pd.Series
        Series satisfying FIXED dependency.
    """
    value_for_entity = {}
    entity_meta = col_meta.copy()
    entity_meta[NULL_PROP] = 0  # avoid nulls for the entity value

    for ent in depend_serie.unique():
        value_for_entity[ent] = generate_column_series(entity_meta, 1, rng).iloc[0]

    return pd.Series(depend_serie.map(value_for_entity), dtype=to_pandas_dtype(col_meta[DATATYPE]))


def generate_dependant_column_series(
    depend_serie: pd.Series,
    col_meta: Dict[str, Any],
    nb_rows: int,
    rng: np.random.Generator,
) -> pd.Series:
    """
    Generate a dependent column while keeping datatype, bounds, and some randomness.

    Supports:
    - BIGGER: each value > depend_serie, still within original min/max bounds
    - MAPPING: values follow valueMap (random choice if multiple)
    - FIXED: value repeats per entity (multi-row)

    Parameters
    ----------
    depend_serie : pd.Series
        Series this column depends on.
    col_meta : dict
        Column metadata, must include DATATYPE, DEPENDENCY_TYPE, and optionally VALUE_MAP.
    nb_rows : int
        Number of rows to generate.
    rng : np.random.Generator
        Random number generator.

    Returns
    -------
    pd.Series
        Generated dependent column series.
    """
    datatype = col_meta[DATATYPE]
    nullable_prop = col_meta.get(NULL_PROP, 0)
    depend_type = col_meta[DEPENDENCY_TYPE]

    if depend_type == DependencyType.BIGGER:
        series = _bigger_series(depend_serie, col_meta, nb_rows, rng)
    elif depend_type == DependencyType.MAPPING:
        series = _mapping_series(depend_serie, col_meta, rng)
    elif depend_type == DependencyType.FIXED:
        series = _fixed_series(depend_serie, col_meta, rng)
    else:
        raise ValueError(f"Unknown dependency type {depend_type} in {col_meta[COL_NAME]}")

    # apply nulls if needed
    return apply_nulls(series, nullable_prop, datatype, rng)


def generate_column(
    name: str,
    columns_meta: List[dict[str, Any]],
    depends_map: Dict[str, str],
    data_dict: Dict[str, pd.Series],
    nb_rows: int,
    rng: np.random.Generator,
    visited: set[str] | None = None,
    max_recursion: int = 10,
    depth: int = 0,
) -> Dict[str, pd.Series]:
    """Recursively generate a column and its dependencies with cycle protection."""
    if visited is None:
        visited = set()

    if name in data_dict:  # Already generated
        return data_dict

    if depth > max_recursion:
        # Too deep: skip dependency, generate column normally
        col_meta = next(cm for cm in columns_meta if cm[COL_NAME] == name)
        data_dict[name] = generate_column_series(col_meta, nb_rows, rng)
        return data_dict

    if name in visited:
        # Detected a circular dependency: ignore dependency
        col_meta = next(cm for cm in columns_meta if cm[COL_NAME] == name)
        data_dict[name] = generate_column_series(col_meta, nb_rows, rng)
        return data_dict

    visited.add(name)
    dep = depends_map.get(name)

    if dep and dep in depends_map:
        # Generate dependency first
        data_dict = generate_column(
            dep,
            columns_meta,
            depends_map,
            data_dict,
            nb_rows,
            rng,
            visited,
            max_recursion,
            depth + 1,
        )
        # Generate dependent column
        col_meta = next(cm for cm in columns_meta if cm[COL_NAME] == name)
        data_dict[name] = generate_dependant_column_series(data_dict[dep], col_meta, nb_rows, rng)
    else:
        # No dependency: generate normally
        col_meta = next(cm for cm in columns_meta if cm[COL_NAME] == name)
        data_dict[name] = generate_column_series(col_meta, nb_rows, rng)

    visited.remove(name)
    return data_dict


def column_group_partitions(
    df: pd.DataFrame,
    columns_group_meta: List[Dict[str, Any]],
) -> pd.DataFrame:
    """Filter a DataFrame so that only rows matching exhaustive column group partitions remain."""
    keep_mask = pd.Series(False, index=df.index)

    for col_group in columns_group_meta:
        if not col_group.get(EXHAUSTIVE_PARTITIONS, False):
            continue
        print("new col group")
        print(col_group)

        partitions = col_group.get(PUBLIC_PARTITIONS, [])
        print("partitions")
        print(partitions)
        for p in partitions:
            print("ppppppppppppp")
            print(p)
            predicate = p[PREDICATE]
            print("predicate")
            print(predicate)
            mask = pd.Series(True, index=df.index)
            for col, v in predicate.items():
                if isinstance(v, dict):
                    if PARTITION_VALUE in v:
                        mask &= df[col] == v[PARTITION_VALUE]
                    else:
                        mask &= (df[col] >= v[LOWER_BOUND]) & (df[col] <= v[UPPER_BOUND])
                else:
                    mask &= df[col] == v

            keep_mask |= mask  # union of all allowed partitions

    return df[keep_mask].reset_index(drop=True)


def make_dummy_from_metadata(
    metadata: Dict[str, Any],
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
    table_schema = metadata.get(TABLE_SCHEMA, {})

    columns_meta = table_schema.get(COL_LIST, [])

    depends_map = {col_meta[COL_NAME]: col_meta.get(DEPENDS_ON) for col_meta in columns_meta}
    columns_group_meta = metadata.get(ADD_INFO, [])
    print(len(columns_group_meta))
    print(columns_group_meta)

    generated: List[pd.DataFrame] = []
    # Oversample to increase chance of covering all partitions
    oversample_rows = int(nb_rows * OVERSAMPLING_FACTOR)

    while sum(len(df) for df in generated) < nb_rows:
        data_dict: Dict[str, pd.Series] = {}
        for col_meta in columns_meta:
            data_dict = generate_column(
                col_meta[COL_NAME],
                columns_meta,
                depends_map,
                data_dict,
                oversample_rows,
                rng,
            )

        df = pd.DataFrame(data_dict)

        if columns_group_meta:
            df = column_group_partitions(df, columns_group_meta)

        generated.append(df)

    output_df = pd.concat(generated, ignore_index=True)
    output_df = output_df.sample(n=nb_rows, random_state=seed)  # final row selection

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
        f"Dummy dataset written to {args.output} "
        f"({len(df_dummy)} rows, {len(df_dummy.columns)} columns)"
    )


if __name__ == "__main__":
    main()
