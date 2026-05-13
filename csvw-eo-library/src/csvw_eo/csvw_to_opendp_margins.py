"""
Convert CSVW-EO JSON metadata into OpenDP margin descriptors.

This module provides:
- A function to translate CSVW-EO differential privacy metadata into
  OpenDP `dp.polars.Margin` objects.
- A CLI for generating margin specifications from a JSON metadata file.

The resulting margins can be used in an OpenDP context, for example:

    dp.Context.compositor(
        data=...,
        privacy_unit=dp.unit_of(contributions=...),
        privacy_loss=dp.loss_of(epsilon=...),
        margins=[...],
    )
"""

from typing import Any

from opendp.extras.polars import Margin

from csvw_eo.constants import (
    ADD_INFO,
    COL_LIST,
    COL_NAME,
    COLUMNS_IN_GROUP,
    INVARIANT_PUBLIC_KEYS,
    MAX_GROUPS,
    MAX_LENGTH,
    MAX_NUM_PARTITIONS,
    PUBLIC_LENGTH,
    TABLE_SCHEMA,
)


def get_margins(col_meta: dict[str, Any], by: list[str]) -> dict[str, Any]:
    """
    Build margin keyword arguments for a given column or column group.

    Parameters
    ----------
    col_meta : Dict[str, Any]
        Metadata describing a column or group of columns, including
        differential privacy constraints (e.g., max_length, max_groups).
    by : List[str]
        Column name(s) to group by when defining the margin.

    Returns
    -------
    Dict[str, Any]
        Dictionary of keyword arguments suitable for constructing an
        OpenDP Margin object.

    """
    margin_kwargs: dict[str, Any] = {"by": by}

    # max_length per column
    if MAX_LENGTH in col_meta:
        margin_kwargs["max_length"] = col_meta[MAX_LENGTH]

    # max_groups per column
    if MAX_GROUPS in col_meta:
        margin_kwargs["max_groups"] = col_meta[MAX_GROUPS]
    elif MAX_NUM_PARTITIONS in col_meta:
        margin_kwargs["max_groups"] = col_meta[MAX_NUM_PARTITIONS]

    # Exhaustive partitions --> invariant keys
    if col_meta.get(INVARIANT_PUBLIC_KEYS):
        margin_kwargs["invariant"] = "keys"

    if col_meta.get(PUBLIC_LENGTH):
        margin_kwargs["invariant"] = "lengths"

    return margin_kwargs


def csvw_to_opendp_margins(csvw_meta: dict[str, Any]) -> list["Margin"]:
    """
    Convert CSVW-EO metadata to a list of OpenDP Margin objects.

    Parameters
    ----------
    csvw_meta : Dict[str, Any]
        CSVW-EO metadata dictionary.

    Returns
    -------
    List["Margin"]
        List of OpenDP margin descriptors.

    Raises
    ------
    ValueError
        If required metadata (e.g., max_contributions) is missing.

    """
    margins: list[Margin] = []

    # Table-level margins: non groupby queries (by=[], max_length=10, ...)
    margin_kwargs: dict[str, Any] = {}

    # Max length (for non count queries)
    if csvw_meta.get(MAX_LENGTH, False):
        margin_kwargs["max_length"] = csvw_meta[MAX_LENGTH]

    # If length is public --> invariant lengths
    if csvw_meta.get(PUBLIC_LENGTH, False):
        margin_kwargs["invariant"] = "lengths"

    if margin_kwargs:
        margins.append(Margin(**margin_kwargs))

    # Column-level margins: groupby queries (by=['col_name'], max_length=100, ...)
    for col_meta in csvw_meta[TABLE_SCHEMA][COL_LIST]:
        margin_kwargs = get_margins(col_meta, by=[col_meta[COL_NAME]])
        margins.append(Margin(**margin_kwargs))

    # Multi-columns-level margins: groupby queries (by=['col_1', 'col_2'], max_length=100, ...)
    for cols_meta in csvw_meta.get(ADD_INFO, []):
        margin_kwargs = get_margins(cols_meta, by=cols_meta[COLUMNS_IN_GROUP])
        margins.append(Margin(**margin_kwargs))

    return margins
