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

from typing import Any

import numpy as np
import pandas as pd

from csvw_safe.constants import (
    COL_NAME,
    DATATYPE,
    DEFAULT_NUMBER_PARTITIONS,
    DEPENDENCY_TYPE,
    EXHAUSTIVE_PARTITIONS,
    KEY_VALUES,
    MAX_NUM_PARTITIONS,
    MAXIMUM,
    MINIMUM,
    NULL_PROP,
    PARTITION_VALUE,
    PREDICATE,
    PUBLIC_PARTITIONS,
    RANDOM_STRINGS,
    VALUE_MAP,
    DependencyType,
)
from csvw_safe.datatypes import (
    XSD_GROUP_MAP,
    DataTypes,
    DataTypesGroups,
    T,
    to_pandas_dtype,
)


def get_bounds(col_meta: dict[str, Any]) -> tuple[T, T]:
    """Get min and max."""
    if MINIMUM not in col_meta:
        raise KeyError(f"Missing {MINIMUM} in column {col_meta[COL_NAME]}")

    if MAXIMUM not in col_meta:
        raise KeyError(f"Missing {MAXIMUM} in column {col_meta[COL_NAME]}")

    return col_meta[MINIMUM], col_meta[MAXIMUM]


def generate_datetime_column(col_meta: dict[str, Any], nb_rows: int, rng: np.random.Generator) -> pd.Series:
    """Generate datetime column between min and max values."""
    lower, upper = get_bounds(col_meta)
    dates = pd.date_range(start=lower, end=upper)
    return pd.Series(rng.choice(dates, size=nb_rows))


def generate_duration_column(col_meta: dict[str, Any], nb_rows: int, rng: np.random.Generator) -> pd.Series:
    """Generate duration column between min and max values."""
    lower, upper = get_bounds(col_meta)

    # assume bounds in seconds (simplest robust approach)
    values = rng.uniform(float(lower), float(upper), size=nb_rows)

    return pd.to_timedelta(values, unit="s")


def generate_integer_column(col_meta: dict[str, Any], nb_rows: int, rng: np.random.Generator) -> pd.Series:
    """Generate numeric column integer between min and max values respecting XSD subtype."""
    lower, upper = get_bounds(col_meta)
    datatype: DataTypes = col_meta[DATATYPE]

    low = int(lower)
    high = int(upper)

    # Force inclusion of zero for if needed
    if datatype == DataTypes.POSITIVE_INTEGER:
        low = max(0, low)
    elif datatype == DataTypes.NEGATIVE_INTEGER:
        high = min(0, high)

    values = rng.integers(low, high + 1, size=nb_rows)

    # Ensure at least one zero if allowed
    if low <= 0 <= high and nb_rows > 0:
        values[0] = 0

    return pd.Series(values, dtype="Int64")


def generate_double_column(col_meta: dict[str, Any], nb_rows: int, rng: np.random.Generator) -> pd.Series:
    """Generate numeric column double between min and max values."""
    lower, upper = get_bounds(col_meta)
    return pd.Series(rng.uniform(float(lower), float(upper), size=nb_rows))


def generate_boolean_column(nb_rows: int, rng: np.random.Generator) -> pd.Series:
    """Generate boolean column."""
    return pd.Series(rng.choice([True, False], size=nb_rows), dtype="boolean")


def generate_string_column(col_meta: dict[str, Any], nb_rows: int, rng: np.random.Generator) -> pd.Series:
    """Generate string column depending on available information."""
    public_keys_values = []
    if KEY_VALUES in col_meta:
        # new key-only format
        for key in col_meta[KEY_VALUES]:
            if isinstance(key, str):
                public_keys_values.append(key)
            else:  # isinstance(key, dict) and PARTITION_VALUE in key:
                public_keys_values.append(key[PARTITION_VALUE])

    elif PUBLIC_PARTITIONS in col_meta:
        for partition in col_meta[PUBLIC_PARTITIONS]:
            if isinstance(partition, str):
                public_keys_values.append(partition)
            else:
                public_keys_values.append(partition[PREDICATE][PARTITION_VALUE])

        if EXHAUSTIVE_PARTITIONS in col_meta and not col_meta[EXHAUSTIVE_PARTITIONS]:
            diff = col_meta[MAX_NUM_PARTITIONS] - len(col_meta[PUBLIC_PARTITIONS])
            public_keys_values.extend(RANDOM_STRINGS[0:diff])

    elif col_meta.get(MAX_NUM_PARTITIONS):
        public_keys_values = RANDOM_STRINGS[0 : col_meta[MAX_NUM_PARTITIONS]]
    else:
        public_keys_values = RANDOM_STRINGS[0:DEFAULT_NUMBER_PARTITIONS]

    return pd.Series(rng.choice(public_keys_values, size=nb_rows))


def generate_column_series(
    col_meta: dict[str, Any],
    nb_rows: int,
    rng: np.random.Generator,
) -> pd.Series:
    """
    Generate a single column series based on metadata.

    Handles datetime, numeric, and partitioned columns, applying nulls.
    """
    datatype: DataTypes = col_meta[DATATYPE]
    group = XSD_GROUP_MAP.get(datatype)

    if group == DataTypesGroups.DATETIME:
        series = generate_datetime_column(col_meta, nb_rows, rng)

    elif group == DataTypesGroups.INTEGER:
        series = generate_integer_column(col_meta, nb_rows, rng)

    elif group == DataTypesGroups.FLOAT:
        series = generate_double_column(col_meta, nb_rows, rng)

    elif group == DataTypesGroups.BOOLEAN:
        series = generate_boolean_column(nb_rows, rng)

    elif group == DataTypesGroups.STRING:
        series = generate_string_column(col_meta, nb_rows, rng)

    elif group == DataTypesGroups.DURATION:
        series = generate_duration_column(col_meta, nb_rows, rng)

    else:
        raise ValueError(f"Unknown datatype {datatype}")

    return series.astype(to_pandas_dtype(datatype))


def _bigger_series(
    depend_serie: pd.Series,
    col_meta: dict[str, Any],
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
    datatype: DataTypes = col_meta[DATATYPE]
    group = XSD_GROUP_MAP[datatype]

    # ---- DATETIME ----
    if group == DataTypesGroups.DATETIME:
        lower_dt, upper_dt = pd.to_datetime(lower), pd.to_datetime(upper)
        total_seconds = (upper_dt - lower_dt).total_seconds()

        offsets: np.ndarray = rng.integers(
            int(0.01 * total_seconds),
            int(0.2 * total_seconds),
            size=nb_rows,
        )

        series = depend_serie + pd.to_timedelta(offsets, unit="s")
        return series.clip(lower=lower_dt, upper=upper_dt)

    # ---- DURATION ----
    if group == DataTypesGroups.DURATION:
        lower_td = pd.to_timedelta(lower)
        upper_td = pd.to_timedelta(upper)

        total_seconds = (upper_td - lower_td).total_seconds()

        offsets = rng.uniform(0.01, 0.2, size=nb_rows) * total_seconds
        series = depend_serie + pd.to_timedelta(offsets, unit="s")

        return series.clip(lower=lower_td, upper=upper_td)

    # ---- INTEGER ----
    if group == DataTypesGroups.INTEGER:
        offsets = rng.integers(int(0.01 * (upper - lower)), int(0.2 * (upper - lower)), size=nb_rows)

        # Ensure depend_serie is integer array
        base = depend_serie.astype("Int64").to_numpy()

        series = pd.Series(base + offsets, index=depend_serie.index, dtype="Int64")
        return series.clip(lower=lower, upper=upper)

    # ---- FLOAT ----
    if group == DataTypesGroups.FLOAT:
        offsets = rng.uniform(0.01, 0.2, size=nb_rows) * float(upper - lower)
        series = pd.Series(
            depend_serie.astype("float64").values + offsets,
            index=depend_serie.index,
            dtype="float64",
        )
        return series.clip(lower=lower, upper=upper)

    raise ValueError(f"BIGGER not supported for datatype {datatype}")


def _mapping_series(
    depend_serie: pd.Series,
    col_meta: dict[str, Any],
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
    col_meta: dict[str, Any],
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
    col_meta: dict[str, Any],
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
    depend_type = col_meta[DEPENDENCY_TYPE]

    if depend_type == DependencyType.BIGGER:
        series = _bigger_series(depend_serie, col_meta, nb_rows, rng)
    elif depend_type == DependencyType.MAPPING:
        series = _mapping_series(depend_serie, col_meta, rng)
    elif depend_type == DependencyType.FIXED:
        series = _fixed_series(depend_serie, col_meta, rng)
    else:
        raise ValueError(f"Unknown dependency type {depend_type} in {col_meta[COL_NAME]}")

    return series


def generate_series(  # noqa: PLR0913
    name: str,
    columns_meta: list[dict[str, Any]],
    depends_map: dict[str, str],
    data_dict: dict[str, pd.Series],
    nb_rows: int,
    rng: np.random.Generator,
    visited: set[str] | None = None,
    max_recursion: int = 10,
    depth: int = 0,
) -> dict[str, pd.Series]:
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
        data_dict = generate_series(
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
