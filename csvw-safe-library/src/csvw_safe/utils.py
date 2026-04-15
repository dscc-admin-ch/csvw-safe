"""Utility files."""

import math
from enum import IntEnum
from typing import Any

import numpy as np


def sanitize(obj: dict[str, Any]) -> dict[str, Any]:
    """
    Recursively convert objects into JSON/CSVW-SAFE serializable types.

    - NumPy scalars → Python scalars
    - NaN or Inf → None
    - Other types remain unchanged
    """
    if isinstance(obj, dict):
        return {k: sanitize(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [sanitize(v) for v in obj]
    if isinstance(obj, np.generic):
        obj = obj.item()  # convert NumPy scalar to native Python

    if isinstance(obj, float):
        if math.isnan(obj) or math.isinf(obj):
            raise ValueError("Value in Nan or infinite")
        return obj  # keep as float

    return obj  # leave everything else unchanged


class ContributionLevel(IntEnum):
    """
    Represents the level at which contribution bounds are applied in CSVW-SAFE metadata.

    Levels:
    - TABLE: global table-level contribution bounds
    - COLUMN: per-column contribution bounds
    - PARTITION: per-partition contribution bounds
    """

    TABLE = 0
    TABLE_WITH_KEYS = 1
    COLUMN = 2
    PARTITION = 3

    @classmethod
    def from_str(cls, value: str) -> "ContributionLevel":
        """
        Convert a string representation into a ContributionLevel enum.

        Parameters
        ----------
        value : str
            One of 'table', 'column', 'partition' (case-insensitive).

        Returns
        -------
        ContributionLevel
            Corresponding enum value.

        Raises
        ------
        ValueError
            If the input string does not match any valid level.

        """
        value = value.lower()
        if value == "table":
            return cls.TABLE
        if value == "table_with_keys":
            return cls.TABLE_WITH_KEYS
        if value == "column":
            return cls.COLUMN
        if value == "partition":
            return cls.PARTITION
        raise ValueError(f"Invalid contribution level: {value}")


def get_effective_contrib_level(
    column_name: str,
    fine_contributions_level: dict[str, ContributionLevel],
    default_contributions_level: ContributionLevel,
) -> ContributionLevel:
    """
    Determine effective contribution level for a column.

    Logic:
      - Take column-specific fine level if it exists, else default.
      - Return the maximum of column and default (table < column < partition).
    """
    fine_level = fine_contributions_level.get(column_name, ContributionLevel.TABLE)
    return max(fine_level, default_contributions_level)


def get_group_contribution_level(
    col_group: list[str],
    fine_contributions_level: dict[str, ContributionLevel],
    default_contributions_level: ContributionLevel,
) -> ContributionLevel:
    """Determine the effective contribution level for a column group."""
    levels = [
        get_effective_contrib_level(col, fine_contributions_level, default_contributions_level)
        for col in col_group
    ]

    if any(level == ContributionLevel.TABLE for level in levels):
        raise ValueError(
            f"Invalid contribution level in ColumnGroup {col_group}: contains TABLE-level column."
        )

    # TABLE < TABLE_WITH_KEYS < COLUMN < PARTITION
    return min(levels)


def prepare_metadata_inputs(
    default_contributions_level: str,
    fine_contributions_level: dict[str, str] | None,
    continuous_partitions: dict[str, list[Any]] | None,
    column_groups: list[list[str]] | None,
) -> tuple[
    ContributionLevel,
    dict[str, ContributionLevel],
    dict[str, list[Any]],
    list[list[str]],
]:
    """
    Normalize optional metadata configuration inputs.

    This helper ensures that optional parameters are initialized with
    appropriate defaults and applies implicit rules required by the
    metadata generation process.

    In particular:
    - Missing dictionaries/lists are replaced with empty structures.
    - Columns with numeric partitions are automatically treated at
      partition-level contribution granularity.

    Parameters
    ----------
    default_contributions_level : str
        Default contribution level applied when no column-specific override exists.
    fine_contributions_level : dict[str, str] or None
        Optional mapping specifying per-column contribution levels.
        Values must be one of {"table", "column", "partition"}.
    continuous_partitions : dict[str, list[Any]] or None
        Mapping of numeric column names to bin boundaries used
        to generate partitions.
    column_groups : list[list[str]] or None
        List of column groups used to create joint partitions.

    Returns
    -------
    tuple
        A tuple containing normalized versions of:
        - default_level : ContributionLevel
        - fine_level : dict[str, ContributionLevel]
        - continuous_partitions : dict[str, list[Any]]
        - column_groups : list[list[str]]

    """
    default_level = ContributionLevel.from_str(default_contributions_level)

    if continuous_partitions is None:
        continuous_partitions = {}

    if column_groups is None:
        column_groups = []

    if fine_contributions_level is None:
        fine_level = {}
    else:
        fine_level = {k: ContributionLevel.from_str(v) for k, v in fine_contributions_level.items()}

    for col in continuous_partitions:  # Continuous bounds default the column to partition level
        fine_level[col] = ContributionLevel.PARTITION

    return default_level, fine_level, continuous_partitions, column_groups
