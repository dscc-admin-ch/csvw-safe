"""
CSVW-EO Dummy Dataset Generator.

This module generates a synthetic dummy dataset from CSVW-EO metadata.
It is intended for testing pipelines and validating metadata structures.

The generator supports:
- categorical partitions
- numeric partitions
- datetime ranges
- nullable proportions
- column groups (joint partitions)

The resulting dataset respects the structural information contained in
CSVW-EO metadata but does not guarantee semantic correctness.
"""

import argparse
import json
import string
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from csvw_eo.constants import (
    ADD_INFO,
    COL_LIST,
    COL_NAME,
    DATATYPE,
    DEPENDS_ON,
    EXHAUSTIVE_KEYS,
    EXHAUSTIVE_PARTITIONS,
    KEY_VALUES,
    LOWER_BOUND,
    NULL_PROP,
    PARTITION_VALUE,
    PREDICATE,
    PUBLIC_PARTITIONS,
    ROW_DEP,
    TABLE_SCHEMA,
    UPPER_BOUND,
)
from csvw_eo.datatypes import XSD_GROUP_MAP, DataTypes, DataTypesGroups
from csvw_eo.generate_series import generate_dataframe

RANDOM_STRINGS = list(string.ascii_lowercase + string.ascii_uppercase + string.digits)


def apply_nulls_serie(
    series: pd.Series,
    nullable_prop: float,
    datatype: DataTypes,
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


def _apply_value_mask(series: pd.Series, value: Any) -> pd.Series:  # noqa: ANN401
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
        group_mask = pd.Series(False, index=df.index)

        if col_group.get(EXHAUSTIVE_PARTITIONS, False):
            # Partitions take precedence over keys
            partitions = col_group.get(PUBLIC_PARTITIONS, [])
            for p in partitions:
                predicate = p.get(PREDICATE, p)
                group_mask |= _predicate_mask(df, predicate)

        elif col_group.get(EXHAUSTIVE_KEYS, False):
            key_values = col_group.get(KEY_VALUES, [])
            for key in key_values:
                group_mask |= _predicate_mask(df, key)

        else:
            continue  # nothing to apply

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


def build_generation_order(depends_map: dict[str, list[dict[str, Any]]]) -> list[str]:
    """
    Compute a deterministic column generation order based on dependencies.

    This function attempts to order columns such that each column appears
    *after* all the columns it depends on (i.e., a topological ordering).

    The input is a mapping of column names to a list of dependency
    specifications. Each dependency dict may contain a `DEPENDS_ON` key
    indicating another column that must be generated first.

    Behavior
    --------
    1. A dependency graph is built where each column maps to the set of
       columns it depends on.
    2. Self-dependencies are ignored.
    3. Columns are iteratively selected:
       - Prefer columns whose dependencies are already resolved.
       - Among those, selection is deterministic (alphabetical order).
    4. If no such column exists (e.g., due to cycles or missing dependencies),
       a fallback strategy is used:
       - Select the column with the fewest dependencies.
       - Break ties alphabetically.

    This ensures the function always returns a complete, deterministic order,
    even when the dependency graph is not a valid DAG.

    Parameters
    ----------
    depends_map : dict[str, list[dict[str, Any]]]
        Mapping of column name to a list of dependency definitions.
        Each dependency dict may include a `DEPENDS_ON` key referencing
        another column.

    Returns
    -------
    list[str]
        A list of column names in generation order. Dependencies will
        appear before dependents whenever possible.

    Notes
    -----
    - Cycles are not explicitly detected or reported. Instead, they are
      handled via the fallback strategy.
    - Missing dependency references (i.e., dependencies not present as keys
      in `depends_map`) are treated as unresolved and may influence ordering.
    - The output is stable and deterministic for a given input.

    """
    # Build graph: col -> set(of columns it depends on)
    graph: dict[str, set[str]] = {
        col: {dep[DEPENDS_ON] for dep in deps if DEPENDS_ON in dep} for col, deps in depends_map.items()
    }

    # remove self-dependencies
    for col, _ in graph.items():
        graph[col].discard(col)

    remaining = set(graph)
    resolved: set[str] = set()
    order: list[str] = []

    while remaining:
        ready = sorted(c for c in remaining if graph[c].issubset(resolved))

        node = ready[0] if ready else min(remaining, key=lambda c: (len(graph[c]), c))

        order.append(node)
        resolved.add(node)
        remaining.remove(node)

    return order


def make_dummy_from_metadata(
    metadata: dict[str, Any],
    nb_rows: int = 100,
    seed: int = 0,
) -> pd.DataFrame:
    """
    Generate a dummy dataset from CSVW-EO metadata, respecting exhaustive column group partitions.

    Parameters
    ----------
    metadata : dict
        CSVW-EO metadata structure.
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
    columns_group_meta = metadata.get(ADD_INFO, [])

    # Based on dependency, create ordered plan for dummy generation
    depends_map: dict[str, list[dict[str, Any]]] = {
        c[COL_NAME]: list(c.get(ROW_DEP, [])) for c in columns_meta
    }
    order = build_generation_order(depends_map)

    # Generate dataframes until enough rows of existing partitions
    generated: list[pd.DataFrame] = []
    while sum(len(df) for df in generated) < nb_rows:
        df = generate_dataframe(
            depends_map,
            order,
            {c[COL_NAME]: c for c in columns_meta},
            nb_rows,
            rng,
        )

        # Remove partition that do ont exist
        if columns_group_meta:
            df = column_group_partitions(df, columns_group_meta)

        generated.append(df)

    # Format in one dataframe with nb_rows
    output_df = pd.concat(generated, ignore_index=True)
    output_df = output_df.sample(n=nb_rows, random_state=seed)

    # Add nulls where required
    output_df = apply_nulls_dataframe(output_df, columns_meta, rng)

    # Metadata column order
    output_df = output_df.reindex(columns=[c[COL_NAME] for c in columns_meta])

    return output_df.reset_index(drop=True)


def main() -> None:
    """Command-line interface for dummy dataset generation."""
    parser = argparse.ArgumentParser(description="Generate a dummy dataset from CSVW-EO metadata.")

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

    print(  # noqa: T201
        f"Dummy dataset written to {args.output} ({len(df_dummy)} rows,{len(df_dummy.columns)} columns)."
    )


if __name__ == "__main__":
    main()
